package dbtype

// 所有已经实现的数据库的类型
const (
	// MySQL 字符集排序规则，默认采用大小写不区分模式
	MySQL = iota
	// MariaDB 字符集排序规则，默认采用大小写不区分模式
	MariaDB
	// SQLServer 字符集排序规则，默认采用大小写不区分模式
	SQLServer
	// Postgres 字符集排序规则，默认采用大小写区分模式
	Postgres
	// OpenGauss 字符集排序规则，默认采用大小写区分模式
	OpenGauss
	// SQLite2 字符集排序规则，默认采用大小写区分模式
	SQLite2
	// SQLite3 字符集排序规则，默认采用大小写区分模式
	SQLite3
	// Oracle 字符集排序规则，默认采用大小写区分模式
	Oracle
	ClickHouse
)
