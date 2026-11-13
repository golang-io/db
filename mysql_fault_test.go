// mysql_fault_test.go 用真实 MySQL 覆盖两类托管连接故障。未设置环境变量时跳过：
//
//	MYSQL_ADDR=host:3306 MYSQL_USER=... MYSQL_PASSWORD=... go test -count=1 -timeout 4m -run 'TestMySQL_' -v .
//
// TestMySQL_BeforeConnect：还没有任何一次拨号成功，GetDB 阻塞，故障排除后同一轮等待返回。
// TestMySQL_AfterConnect：池已经发布，GetDB 立即返回同一个池，错误出现在链式调用末尾。
// TestMySQL_OpenDBWrongPassword：OpenDB 失败即返回，不进入托管重试。
//
// 管理账号只创建、修改和删除专用测试账号。登录后先检查权限，不足则直接失败。
// 端口中断由本机 TCP 代理模拟，不停止数据库进程。
package db_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/golang-io/db"
	_ "github.com/golang-io/db/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// liveMySQL 是测试用的管理连接，不经过本库的托管拨号。
type liveMySQL struct {
	addr  string
	admin *sql.DB
}

// mysqlAccount 是本用例专用的账号和库，结束时删除。
type mysqlAccount struct {
	user string
	pass string
	db   string
}

// errorLog 记录托管拨号失败，供用例确认失败已经发生、GetDB 仍未返回。
type errorLog struct {
	mu   sync.Mutex
	errs []string
}

func (l *errorLog) Info(context.Context, string, ...any) {}
func (l *errorLog) Warn(context.Context, string, ...any) {}
func (l *errorLog) Error(_ context.Context, msg string, args ...any) {
	l.mu.Lock()
	l.errs = append(l.errs, strings.TrimSuffix(fmt.Sprintln(append([]any{msg}, args...)...), "\n"))
	l.mu.Unlock()
}

func (l *errorLog) last() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.errs) == 0 {
		return "", false
	}
	return l.errs[len(l.errs)-1], true
}

// tcpProxy 把本机端口转发到真实 MySQL。Halt 关闭监听和已建立的连接，Resume 在同一地址重新监听。
type tcpProxy struct {
	upstream string
	addr     string

	mu      sync.Mutex
	ln      net.Listener
	conns   map[net.Conn]struct{}
	stopped bool
	closed  bool
}

// openDirectMySQL 打开不经过本库托管机制的原生 *sql.DB 连接，统一配置 5s 建连与读写超时。
func openDirectMySQL(addr, user, pass string) (*sql.DB, error) {
	cfg := gomysql.Config{
		User:                 user,
		Passwd:               pass,
		Net:                  "tcp",
		Addr:                 addr,
		Timeout:              5 * time.Second,
		ReadTimeout:          5 * time.Second,
		WriteTimeout:         5 * time.Second,
		AllowNativePasswords: true,
	}
	return sql.Open("mysql", cfg.FormatDSN())
}

func requireLiveMySQL(t *testing.T) liveMySQL {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("MYSQL_ADDR"))
	user := strings.TrimSpace(os.Getenv("MYSQL_USER"))
	pass := os.Getenv("MYSQL_PASSWORD")
	if addr == "" || user == "" || pass == "" {
		t.Skip("set MYSQL_ADDR, MYSQL_USER and MYSQL_PASSWORD to run real MySQL fault tests")
	}
	admin, err := openDirectMySQL(addr, user, pass)
	if err != nil {
		t.Fatal(err)
	}
	admin.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("admin ping %s: %v", addr, err)
	}
	if err := checkAdminPrivileges(admin); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	if err := checkKillPrivilege(addr, admin); err != nil {
		_ = admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	return liveMySQL{addr: addr, admin: admin}
}

// checkAdminPrivileges 在创建测试账号之前确认管理账号的全局权限。
// MySQL 没有单独的 ALTER USER 权限；修改和删除用户都属于 CREATE USER。
func checkAdminPrivileges(admin *sql.DB) error {
	rows, err := admin.Query("SHOW GRANTS")
	if err != nil {
		return fmt.Errorf("show grants: %w", err)
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return fmt.Errorf("scan grants: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read grants: %w", err)
	}
	missing := missingAdminPrivileges(grants)
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("admin account lacks privileges:\n- %s\ncurrent grants:\n%s",
		strings.Join(missing, "\n- "), strings.Join(grants, "\n"))
}

// missingAdminPrivileges 从 SHOW GRANTS 的全局授权里找出这组测试缺少的权限。
// 只认 ON *.*：某个业务库上的 ALL 不能用来新建随机测试库、新建账号或跨会话 KILL。
// 管理账号除了建库、删库、管理测试账号（CREATE / DROP / CREATE USER），还需要在 createAccount 中
// 向测试账号授予库级 CREATE、SELECT、INSERT，因此自身必须同时持有 SELECT、INSERT 及静态全局 GRANT OPTION。
func missingAdminPrivileges(grants []string) []string {
	var priv adminPrivs
	priv.names = map[string]bool{}
	for _, grant := range grants {
		priv.merge(grant)
	}
	var missing []string
	if !priv.has("CREATE USER") {
		missing = append(missing, "CREATE USER（创建、修改和删除用户）")
	}
	if !priv.has("CREATE") {
		missing = append(missing, "CREATE（创建数据库并向测试账号授建表权限）")
	}
	if !priv.has("DROP") {
		missing = append(missing, "DROP（删除数据库）")
	}
	if !priv.has("SELECT") {
		missing = append(missing, "SELECT（向测试账号授查询权限）")
	}
	if !priv.has("INSERT") {
		missing = append(missing, "INSERT（向测试账号授写入权限）")
	}
	if !priv.staticGrantOption {
		missing = append(missing, "GRANT OPTION（把库权限授给测试账号）")
	}
	return missing
}

// checkKillPrivilege 创建临时无特权探测账号并保持一条存活物理连接（sql.Conn），验证管理账号能否跨用户执行 KILL。
// MySQL 允许普通账号 KILL 自身会话，且对不存在的线程 ID（thread ID）会先返回 1094（Unknown thread id）而不校验跨会话权限；
// 同时不能仅凭 SHOW GRANTS 文本判断 SUPER / CONNECTION_ADMIN（部分云数据库或角色授权不会直接列出），
// 因此必须针对其他用户的真实连接实测：无权限时服务端返回 1095（You are not owner of thread）。
func checkKillPrivilege(addr string, admin *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 临时账号名控制在 16 字符以内，仅拥有默认 USAGE 权限，无需建库即可登录。
	user := "flt_" + randomHex(6)
	pass := randomPassword()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass)); err != nil {
		return fmt.Errorf("create kill-probe user: %w", err)
	}
	// 仅在 CREATE USER 成功后注册清理；利用 defer 后进先出（LIFO）顺序，先关闭探测连接再删除账号。
	defer func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = admin.ExecContext(cleanCtx, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
	}()

	probeDB, err := openDirectMySQL(addr, user, pass)
	if err != nil {
		return fmt.Errorf("open kill-probe db: %w", err)
	}
	defer func() { _ = probeDB.Close() }()

	// 独占持有单条物理连接（*sql.Conn），确保在执行 KILL 前该会话处于存活状态且不会被连接池回收或切换。
	conn, err := probeDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("connect kill-probe user: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// MySQL 8.0+ 的线程 ID（my_thread_id）为 64 位无符号整数，使用 uint64 接收避免溢出。
	var connID uint64
	if err := conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connID); err != nil {
		return fmt.Errorf("query kill-probe connection id: %w", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf("KILL %d", connID)); err != nil {
		return fmt.Errorf("admin account cannot KILL other sessions: %w", err)
	}
	return nil
}

// mysqlStaticPrivileges 是 MySQL 静态全局权限（存储于 mysql.user 表）的完整集合。
// MySQL 8.0+ 将静态全局权限与动态全局权限（如 CONNECTION_ADMIN，存储于 mysql.global_grants 表）分行展示：
// 动态权限行上的 WITH GRANT OPTION 只允许转授该动态权限，不代表 mysql.user.Grant_priv='Y'，不能用于授予库表静态权限。
var mysqlStaticPrivileges = map[string]bool{
	"ALL":                     true,
	"ALL PRIVILEGES":          true,
	"ALTER":                   true,
	"ALTER ROUTINE":           true,
	"CREATE":                  true,
	"CREATE ROLE":             true,
	"CREATE ROUTINE":          true,
	"CREATE TABLESPACE":       true,
	"CREATE TEMPORARY TABLES": true,
	"CREATE USER":             true,
	"CREATE VIEW":             true,
	"DELETE":                  true,
	"DROP":                    true,
	"DROP ROLE":               true,
	"EVENT":                   true,
	"EXECUTE":                 true,
	"FILE":                    true,
	"GRANT OPTION":            true,
	"INDEX":                   true,
	"INSERT":                  true,
	"LOCK TABLES":             true,
	"PROCESS":                 true,
	"REFERENCES":              true,
	"RELOAD":                  true,
	"REPLICATION CLIENT":      true,
	"REPLICATION SLAVE":       true,
	"SELECT":                  true,
	"SHOW DATABASES":          true,
	"SHOW VIEW":               true,
	"SHUTDOWN":                true,
	"SUPER":                   true,
	"TRIGGER":                 true,
	"UPDATE":                  true,
	"USAGE":                   true,
}

// adminPrivs 是全局授权的并集。all 表示静态全局 ALL PRIVILEGES，staticGrantOption 表示静态全局权限可转授。
type adminPrivs struct {
	all               bool
	staticGrantOption bool
	names             map[string]bool
}

func (p *adminPrivs) merge(grant string) {
	privs, grantOption, ok := globalPrivileges(grant)
	if !ok {
		return
	}
	if privs == "ALL" || privs == "ALL PRIVILEGES" {
		p.all = true
		if grantOption {
			p.staticGrantOption = true
		}
		return
	}
	hasStatic := false
	for _, name := range strings.Split(privs, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		p.names[name] = true
		if name == "GRANT OPTION" {
			p.staticGrantOption = true
		}
		if mysqlStaticPrivileges[name] {
			hasStatic = true
		}
	}
	if grantOption && hasStatic {
		p.staticGrantOption = true
	}
}

func (p adminPrivs) has(name string) bool {
	return p.all || p.names[name]
}

// globalPrivileges 提取 "GRANT ... ON *.*" 中的权限列表。库级授权返回 ok=false。
func globalPrivileges(grant string) (privs string, grantOption bool, ok bool) {
	upper := strings.ToUpper(strings.TrimSpace(grant))
	if !strings.HasPrefix(upper, "GRANT ") {
		return "", false, false
	}
	idx := strings.Index(upper, " ON *.*")
	if idx < 0 {
		idx = strings.Index(upper, " ON `*`.*")
	}
	if idx < 0 {
		return "", false, false
	}
	return strings.TrimSpace(upper[len("GRANT "):idx]), strings.Contains(upper[idx:], "WITH GRANT OPTION"), true
}

func (m liveMySQL) createAccount(t *testing.T) mysqlAccount {
	t.Helper()
	id := randomHex(6)
	account := mysqlAccount{user: "flt_" + id, pass: randomPassword(), db: "flt_" + id}
	mustAdmin(t, m.admin, "CREATE DATABASE `"+account.db+"`")
	t.Cleanup(func() {
		_, _ = m.admin.Exec("DROP DATABASE IF EXISTS `" + account.db + "`")
	})
	mustAdmin(t, m.admin, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", account.user, account.pass))
	t.Cleanup(func() {
		_, _ = m.admin.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", account.user))
	})
	// 仅授予故障测试实际所需的最小库级权限（CREATE TABLE / SELECT / INSERT），与 missingAdminPrivileges 预检项保持一致，
	// 避免最小权限管理账号因自身不持有全部库级权限而在执行 GRANT ALL PRIVILEGES 时报 Error 1044。
	mustAdmin(t, m.admin, fmt.Sprintf("GRANT CREATE, SELECT, INSERT ON `%s`.* TO '%s'@'%%'", account.db, account.user))
	return account
}

func (m liveMySQL) setPassword(t *testing.T, user, pass string) {
	t.Helper()
	mustAdmin(t, m.admin, fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass))
}

// restorePassword 用于清理阶段。清理函数里不能 FailNow，否则后面的删账号步骤不会执行。
func (m liveMySQL) restorePassword(t *testing.T, user, pass string) {
	t.Helper()
	if _, err := m.admin.Exec(fmt.Sprintf("ALTER USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass)); err != nil {
		t.Errorf("restore password for %s: %v", user, err)
	}
}

func mustAdmin(t *testing.T, admin *sql.DB, query string) {
	t.Helper()
	if _, err := admin.Exec(query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// appOptions 把超时写进 Options，由 mysql 工厂回填到 DSN 的零值。
// 读写超时设短，避免把“对端已断开”误判成用例卡死。
func appOptions(name, rawURL string, log db.Logger) db.Options {
	return db.Options{
		Name:            name,
		URL:             rawURL,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     2 * time.Second,
		WriteTimeout:    2 * time.Second,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifeTime: time.Hour,
		ConnMaxIdleTime: time.Hour,
		LogLevel:        logger.Silent,
		Output:          io.Discard,
		Log:             log,
	}
}

func mysqlURL(user, pass, addr, database string) string {
	cfg := gomysql.Config{
		User:                 user,
		Passwd:               pass,
		Net:                  "tcp",
		Addr:                 addr,
		DBName:               database,
		AllowNativePasswords: true,
	}
	return "mysql://" + cfg.FormatDSN()
}

func mustCreateDB(t *testing.T, opts db.Options) {
	t.Helper()
	if err := db.CreateDB(opts); err != nil {
		t.Fatal(err)
	}
}

// startGetDB 在后台等待托管连接。done 在 GetDB 返回后关闭，清理阶段用它确认没有永久停住的等待。
func startGetDB(name string) (<-chan *gorm.DB, <-chan struct{}) {
	ch := make(chan *gorm.DB, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ch <- db.GetDB(context.Background(), name)
	}()
	return ch, done
}

// waitDialFailure 等到第一次拨号失败被记下来，同时确认 GetDB 还没有返回。
func waitDialFailure(t *testing.T, returned <-chan *gorm.DB, log *errorLog) time.Time {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-returned:
			t.Fatal("GetDB returned while the database was still unavailable")
		default:
		}
		if msg, ok := log.last(); ok {
			t.Logf("dial failed and GetDB is still waiting: %s", msg)
			return time.Now()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("dial did not report an error")
	return time.Time{}
}

// waitReturned 要求故障排除后 GetDB 返回，并且返回时间落在默认重试间隔之后。
// 失败日志刚写下就会进入 10s 睡眠，这里用 8s 作为下限，留出调度误差。
func waitReturned(t *testing.T, returned <-chan *gorm.DB, failedAt time.Time) *gorm.DB {
	t.Helper()
	select {
	case g := <-returned:
		if waited := time.Since(failedAt); waited < 8*time.Second {
			t.Fatalf("GetDB returned %v after the dial failure, want it to wait out the retry interval", waited)
		} else {
			t.Logf("GetDB returned after %v", waited)
		}
		return g
	case <-time.After(20 * time.Second):
		t.Fatal("GetDB still waiting after the fault was repaired")
	}
	return nil
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(25 * time.Second):
		t.Errorf("GetDB did not return during cleanup")
	}
}

// queryOne 走链式调用，错误留在末尾的 Error。
func queryOne(g *gorm.DB) error {
	var n int64
	if err := g.Raw("SELECT 1").Scan(&n).Error; err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("SELECT 1 = %d", n)
	}
	return nil
}

// queryReady 允许第一次使用撞上池里的死连接；这次失败后池必须还能完成查询。
func queryReady(t *testing.T, g *gorm.DB) {
	t.Helper()
	if err := queryOne(g); err != nil {
		t.Logf("first query saw a stale connection: %v", err)
		if err := queryOne(g); err != nil {
			t.Fatal(err)
		}
	}
}

func mustQuery(t *testing.T, g *gorm.DB) {
	t.Helper()
	if err := queryOne(g); err != nil {
		t.Fatal(err)
	}
}

func mustFailQuery(t *testing.T, g *gorm.DB) error {
	t.Helper()
	err := queryOne(g)
	if err == nil {
		t.Fatal("query succeeded")
	}
	return err
}

// dropIdle 丢掉池里已经登录的连接，下一次查询必须重新握手。
// SetMaxIdleConns(0) 会立刻关闭空闲连接；短暂调小存活时间淘汰在途归还连接后，恢复 appOptions 的池参数。
func dropIdle(t *testing.T, g *gorm.DB) {
	t.Helper()
	sqlDB, err := g.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxIdleConns(0)
	sqlDB.SetConnMaxIdleTime(time.Millisecond)
	sqlDB.SetConnMaxLifetime(time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxIdleTime(time.Hour)
	sqlDB.SetConnMaxLifetime(time.Hour)
}

func poolPtr(t *testing.T, g *gorm.DB) *sql.DB {
	t.Helper()
	sqlDB, err := g.DB()
	if err != nil {
		t.Fatal(err)
	}
	return sqlDB
}

func isAccessDenied(err error) bool {
	var me *gomysql.MySQLError
	if errors.As(err, &me) && me.Number == 1045 {
		return true
	}
	// GORM 或驱动有时只把错误文本包进新错误，1045 仍是认证失败的稳定标记。
	return err != nil && strings.Contains(err.Error(), "1045")
}

func countRows(t *testing.T, g *gorm.DB) int64 {
	t.Helper()
	var n int64
	var err error
	for range 2 {
		err = g.Raw("SELECT COUNT(*) FROM t").Scan(&n).Error
		if err == nil {
			return n
		}
	}
	t.Fatal(err)
	return 0
}

func startTCPProxy(t *testing.T, upstream string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{
		upstream: upstream,
		addr:     ln.Addr().String(),
		ln:       ln,
		conns:    map[net.Conn]struct{}{},
	}
	go p.accept(ln)
	t.Cleanup(p.Close)
	return p
}

func (p *tcpProxy) Addr() string { return p.addr }

func (p *tcpProxy) accept(ln net.Listener) {
	for {
		client, err := ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		reject := p.stopped || p.closed
		if !reject {
			p.conns[client] = struct{}{}
		}
		p.mu.Unlock()
		if reject {
			_ = client.Close()
			continue
		}
		go p.forward(client)
	}
}

func (p *tcpProxy) forward(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", p.upstream, 3*time.Second)
	if err != nil {
		p.forget(client)
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.stopped || p.closed {
		p.mu.Unlock()
		_ = upstream.Close()
		p.forget(client)
		_ = client.Close()
		return
	}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		_, _ = io.Copy(upstream, client)
		_ = upstream.Close()
		wg.Done()
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		wg.Done()
	}()
	wg.Wait()
	p.forget(client)
	p.forget(upstream)
}

func (p *tcpProxy) forget(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// Halt 让后续拨号得到 connection refused，并掐断已经转发的连接。
func (p *tcpProxy) Halt() {
	p.mu.Lock()
	p.stopped = true
	ln := p.ln
	p.ln = nil
	conns := p.conns
	p.conns = map[net.Conn]struct{}{}
	p.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for c := range conns {
		_ = c.Close()
	}
}

// Resume 在原来的地址上重新监听。旧监听刚关闭时可能短暂占用端口，因此重试几次。
func (p *tcpProxy) Resume() error {
	p.mu.Lock()
	if p.closed || !p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	var ln net.Listener
	var err error
	for range 20 {
		ln, err = net.Listen("tcp", p.addr)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.stopped {
		_ = ln.Close()
		return nil
	}
	p.ln = ln
	p.stopped = false
	go p.accept(ln)
	return nil
}

func (p *tcpProxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.stopped = true
	ln := p.ln
	p.ln = nil
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for c := range conns {
		_ = c.Close()
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func randomPassword() string {
	// 带上大小写、数字和符号，避开 SQL 引号，降低密码策略拒绝的可能。
	return "Aa1_" + randomHex(12)
}

// TestMySQL_BeforeConnect 覆盖第一次拨号成功之前的阻塞重试。
func TestMySQL_BeforeConnect(t *testing.T) {
	live := requireLiveMySQL(t)
	t.Run("wrong password", func(t *testing.T) { wrongPasswordThenFixed(t, live) })
	t.Run("port refused", func(t *testing.T) { portRefusedThenAccept(t, live) })
	t.Run("concurrent waiters", func(t *testing.T) { concurrentGetDBWaits(t, live) })
}

// TestMySQL_AfterConnect 覆盖连接池发布之后的故障。GetDB 不阻塞，池不替换。
func TestMySQL_AfterConnect(t *testing.T) {
	live := requireLiveMySQL(t)
	t.Run("password changed", func(t *testing.T) { passwordChangedAfterConnect(t, live) })
	t.Run("password restored", func(t *testing.T) { passwordChangedThenRestored(t, live) })
	t.Run("proxy outage", func(t *testing.T) { proxyOutageThenRecover(t, live) })
	t.Run("kill connection", func(t *testing.T) { serverKillsConnection(t, live) })
	t.Run("transaction disconnect", func(t *testing.T) { transactionBrokenByDisconnect(t, live) })
}

// wrongPasswordThenFixed 密码错误时 GetDB 不返回；改对后同一轮等待结束并可查询。
func wrongPasswordThenFixed(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	correct := account.pass
	live.setPassword(t, account.user, randomPassword())

	name := uniq("mysql-wrong-pass")
	log := &errorLog{}
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, correct, live.addr, account.db), log))
	returned, done := startGetDB(name)
	t.Cleanup(func() {
		waitDone(t, done)
		_ = db.Get(name).Close()
	})
	// 清理时先把密码改回 DSN 中的值，阻塞中的 GetDB 才能在 Close 之前返回。
	t.Cleanup(func() { live.restorePassword(t, account.user, correct) })

	failedAt := waitDialFailure(t, returned, log)
	live.setPassword(t, account.user, correct)
	g := waitReturned(t, returned, failedAt)
	mustQuery(t, g)
}

// passwordChangedAfterConnect 已登录的连接仍然可用；丢掉空闲连接后，新握手在链尾返回 1045。
func passwordChangedAfterConnect(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	name := uniq("mysql-pass-changed")
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, account.pass, live.addr, account.db), nil))
	t.Cleanup(func() { _ = db.Get(name).Close() })

	g := db.GetDB(context.Background(), name)
	mustQuery(t, g)
	pool := poolPtr(t, g)

	live.setPassword(t, account.user, randomPassword())
	mustQuery(t, g)

	dropIdle(t, g)
	err := mustFailQuery(t, g)
	if !isAccessDenied(err) {
		t.Fatalf("want access denied after a new handshake, got %v", err)
	}
	same := db.GetDB(context.Background(), name)
	if poolPtr(t, same) != pool {
		t.Fatal("password change replaced the pool")
	}
}

// proxyOutageThenRecover 目标地址中断时查询失败且 GetDB 不阻塞，恢复后原池可以查询。
func proxyOutageThenRecover(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	proxy := startTCPProxy(t, live.addr)
	name := uniq("mysql-outage")
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, account.pass, proxy.Addr(), account.db), nil))
	t.Cleanup(func() { _ = db.Get(name).Close() })

	g := db.GetDB(context.Background(), name)
	mustQuery(t, g)
	pool := poolPtr(t, g)

	proxy.Halt()
	_ = mustFailQuery(t, g)
	done := make(chan *gorm.DB, 1)
	go func() { done <- db.GetDB(context.Background(), name) }()
	select {
	case same := <-done:
		if poolPtr(t, same) != pool {
			t.Fatal("outage replaced the pool")
		}
	case <-time.After(time.Second):
		t.Fatal("GetDB blocked during outage")
	}

	if err := proxy.Resume(); err != nil {
		t.Fatal(err)
	}
	queryReady(t, db.GetDB(context.Background(), name))
	if poolPtr(t, db.GetDB(context.Background(), name)) != pool {
		t.Fatal("recovery replaced the pool")
	}
}

// passwordChangedThenRestored 新握手因改密失败，改回后同一个池恢复查询。
func passwordChangedThenRestored(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	name := uniq("mysql-pass-restored")
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, account.pass, live.addr, account.db), nil))
	t.Cleanup(func() { _ = db.Get(name).Close() })
	t.Cleanup(func() { live.restorePassword(t, account.user, account.pass) })

	g := db.GetDB(context.Background(), name)
	mustQuery(t, g)
	pool := poolPtr(t, g)

	// 先确认旧会话还在，再丢掉它，避免这一条和“改密后旧连接仍可用”混在一起。
	live.setPassword(t, account.user, randomPassword())
	mustQuery(t, g)
	dropIdle(t, g)
	err := mustFailQuery(t, g)
	if !isAccessDenied(err) {
		t.Fatalf("want access denied, got %v", err)
	}

	live.setPassword(t, account.user, account.pass)
	queryReady(t, g)
	if poolPtr(t, db.GetDB(context.Background(), name)) != pool {
		t.Fatal("restoring the password replaced the pool")
	}
}

// portRefusedThenAccept 端口拒绝时 GetDB 阻塞，开始监听后从同一轮等待返回。
func portRefusedThenAccept(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	proxy := startTCPProxy(t, live.addr)
	proxy.Halt()

	name := uniq("mysql-refused")
	log := &errorLog{}
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, account.pass, proxy.Addr(), account.db), log))
	returned, done := startGetDB(name)
	t.Cleanup(func() {
		waitDone(t, done)
		_ = db.Get(name).Close()
	})
	t.Cleanup(func() {
		if err := proxy.Resume(); err != nil {
			t.Errorf("resume proxy: %v", err)
		}
	})

	failedAt := waitDialFailure(t, returned, log)
	if err := proxy.Resume(); err != nil {
		t.Fatal(err)
	}
	g := waitReturned(t, returned, failedAt)
	mustQuery(t, g)
}

// serverKillsConnection 服务端关掉当前会话。下一次使用可能先看到死连接，随后查询成功。
func serverKillsConnection(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	name := uniq("mysql-kill")
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, account.pass, live.addr, account.db), nil))
	t.Cleanup(func() { _ = db.Get(name).Close() })

	g := db.GetDB(context.Background(), name)
	mustQuery(t, g)
	// MySQL 8.0+ 的线程 ID（my_thread_id）为 64 位无符号整数。
	var connID uint64
	if err := g.Raw("SELECT CONNECTION_ID()").Scan(&connID).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := live.admin.Exec(fmt.Sprintf("KILL %d", connID)); err != nil {
		t.Fatalf("KILL %d: %v", connID, err)
	}
	queryReady(t, g)
}

// transactionBrokenByDisconnect 事务中途断开后写入回滚，恢复后新事务可以提交。
func transactionBrokenByDisconnect(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	proxy := startTCPProxy(t, live.addr)
	name := uniq("mysql-tx-break")
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, account.pass, proxy.Addr(), account.db), nil))
	t.Cleanup(func() { _ = db.Get(name).Close() })

	g := db.GetDB(context.Background(), name)
	if err := g.Exec("CREATE TABLE t (id INT PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	err := g.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("INSERT INTO t (id) VALUES (1)").Error; err != nil {
			return err
		}
		proxy.Halt()
		return tx.Exec("INSERT INTO t (id) VALUES (2)").Error
	})
	if err == nil {
		t.Fatal("transaction succeeded after the connection was dropped")
	}
	t.Logf("broken transaction: %v", err)

	if err := proxy.Resume(); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, db.GetDB(context.Background(), name)); n != 0 {
		t.Fatalf("rolled back transaction left %d rows", n)
	}
	if err := db.GetDB(context.Background(), name).Transaction(func(tx *gorm.DB) error {
		return tx.Exec("INSERT INTO t (id) VALUES (3)").Error
	}); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, db.GetDB(context.Background(), name)); n != 1 {
		t.Fatalf("committed rows = %d, want 1", n)
	}
}

// concurrentGetDBWaits 多个 GetDB 一起等第一次拨号，恢复后拿到同一个池。
func concurrentGetDBWaits(t *testing.T, live liveMySQL) {
	account := live.createAccount(t)
	correct := account.pass
	live.setPassword(t, account.user, randomPassword())

	name := uniq("mysql-concurrent")
	log := &errorLog{}
	mustCreateDB(t, appOptions(name, mysqlURL(account.user, correct, live.addr, account.db), log))
	type wait struct {
		ch   <-chan *gorm.DB
		done <-chan struct{}
	}
	waits := make([]wait, 3)
	for i := range waits {
		waits[i].ch, waits[i].done = startGetDB(name)
	}
	t.Cleanup(func() {
		for _, w := range waits {
			waitDone(t, w.done)
		}
		_ = db.Get(name).Close()
	})
	t.Cleanup(func() { live.restorePassword(t, account.user, correct) })

	failedAt := waitDialFailure(t, waits[0].ch, log)
	for _, w := range waits[1:] {
		select {
		case <-w.ch:
			t.Fatal("GetDB returned while the password was still wrong")
		default:
		}
	}
	live.setPassword(t, account.user, correct)

	got := make([]*gorm.DB, len(waits))
	for i, w := range waits {
		got[i] = waitReturned(t, w.ch, failedAt)
	}
	first := poolPtr(t, got[0])
	for i := 1; i < len(got); i++ {
		if poolPtr(t, got[i]) != first {
			t.Fatal("waiters received different pools")
		}
	}
	mustQuery(t, got[0])
}

// TestMySQL_OpenDBWrongPassword OpenDB 密码错误时立即返回错误，不进入托管重试。
func TestMySQL_OpenDBWrongPassword(t *testing.T) {
	live := requireLiveMySQL(t)
	account := live.createAccount(t)
	opts := appOptions("default", mysqlURL(account.user, randomPassword(), live.addr, account.db), nil)

	start := time.Now()
	g, err := db.OpenDB(context.Background(), opts)
	elapsed := time.Since(start)
	if g != nil {
		if sqlDB, dbErr := g.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	}
	if err == nil {
		t.Fatal("OpenDB succeeded with a wrong password")
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("OpenDB took %v, want a direct failure", elapsed)
	}
	if !isAccessDenied(err) {
		t.Fatalf("want access denied, got %v", err)
	}
	t.Logf("OpenDB failed in %v: %v", elapsed, err)
}

// TestMySQL_KillPrivilegeCheck 验证仅具备 CREATE USER 但缺少跨会话 KILL 权限的账号会被 checkKillPrivilege 拦截（Error 1095）。
func TestMySQL_KillPrivilegeCheck(t *testing.T) {
	live := requireLiveMySQL(t)
	user := "flt_" + randomHex(6)
	pass := randomPassword()
	mustAdmin(t, live.admin, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass))
	t.Cleanup(func() {
		_, _ = live.admin.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
	})
	mustAdmin(t, live.admin, fmt.Sprintf("GRANT CREATE USER ON *.* TO '%s'@'%%'", user))

	limitedAdmin, err := openDirectMySQL(live.addr, user, pass)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = limitedAdmin.Close() })

	err = checkKillPrivilege(live.addr, limitedAdmin)
	if err == nil {
		t.Fatal("checkKillPrivilege succeeded for account without SUPER or CONNECTION_ADMIN")
	}
	var me *gomysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1095 {
		t.Fatalf("want MySQL error 1095 (not owner of thread), got %v", err)
	}
	t.Logf("checkKillPrivilege rejected limited admin as expected: %v", err)
}

// TestMySQL_AdminPrivilegeCheck 验证 checkAdminPrivileges 预检通过的最小静态权限账号可执行完整 createAccount 建表读写，
// 且缺少 SELECT/INSERT 或仅拥有动态权限 WITH GRANT OPTION 的账号会被预检拦截。
func TestMySQL_AdminPrivilegeCheck(t *testing.T) {
	live := requireLiveMySQL(t)

	newTempAdmin := func(t *testing.T, grantSQL ...string) *sql.DB {
		t.Helper()
		user := "flt_" + randomHex(6)
		pass := randomPassword()
		mustAdmin(t, live.admin, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass))
		t.Cleanup(func() {
			_, _ = live.admin.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
		})
		for _, q := range grantSQL {
			mustAdmin(t, live.admin, fmt.Sprintf(q, user))
		}
		conn, err := openDirectMySQL(live.addr, user, pass)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}

	t.Run("minimal valid privileges", func(t *testing.T) {
		minAdmin := newTempAdmin(t,
			"GRANT SELECT, INSERT, CREATE, DROP, CREATE USER ON *.* TO '%s'@'%%' WITH GRANT OPTION",
		)
		if err := checkAdminPrivileges(minAdmin); err != nil {
			t.Fatalf("minimal admin rejected: %v", err)
		}
		account := liveMySQL{addr: live.addr, admin: minAdmin}.createAccount(t)
		g, err := db.OpenDB(context.Background(), appOptions("default", mysqlURL(account.user, account.pass, live.addr, account.db), nil))
		if err != nil {
			t.Fatalf("open test account: %v", err)
		}
		t.Cleanup(func() { _ = poolPtr(t, g).Close() })
		if err := g.Exec("CREATE TABLE t (id INT PRIMARY KEY)").Error; err != nil {
			t.Fatalf("create table with delegated privileges: %v", err)
		}
		if err := g.Exec("INSERT INTO t (id) VALUES (1)").Error; err != nil {
			t.Fatalf("insert row with delegated privileges: %v", err)
		}
		if n := countRows(t, g); n != 1 {
			t.Fatalf("count = %d, want 1", n)
		}
	})

	t.Run("missing select and insert", func(t *testing.T) {
		badAdmin := newTempAdmin(t,
			"GRANT CREATE, DROP, CREATE USER ON *.* TO '%s'@'%%' WITH GRANT OPTION",
		)
		err := checkAdminPrivileges(badAdmin)
		if err == nil || !strings.Contains(err.Error(), "SELECT") || !strings.Contains(err.Error(), "INSERT") {
			t.Fatalf("want missing SELECT and INSERT error, got %v", err)
		}
	})

	t.Run("dynamic grant option without static grant option", func(t *testing.T) {
		user := "flt_" + randomHex(6)
		pass := randomPassword()
		mustAdmin(t, live.admin, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", user, pass))
		t.Cleanup(func() {
			_, _ = live.admin.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
		})
		mustAdmin(t, live.admin, fmt.Sprintf("GRANT SELECT, INSERT, CREATE, DROP, CREATE USER ON *.* TO '%s'@'%%'", user))
		if _, err := live.admin.Exec(fmt.Sprintf("GRANT CONNECTION_ADMIN ON *.* TO '%s'@'%%' WITH GRANT OPTION", user)); err != nil {
			t.Skipf("server or admin does not support granting dynamic CONNECTION_ADMIN: %v", err)
		}
		dynAdmin, err := openDirectMySQL(live.addr, user, pass)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dynAdmin.Close() })

		err = checkAdminPrivileges(dynAdmin)
		if err == nil || !strings.Contains(err.Error(), "GRANT OPTION") {
			t.Fatalf("want missing static GRANT OPTION error, got %v", err)
		}
	})
}
