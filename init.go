// Package orm
package orm

import "github.com/assembly-hub/orm/dbtype"

type dbCoreData struct {
	DBType    int
	StrEsc    string
	EscStart  string
	EscEnd    string
	Esc       string
	BinStr    string
	IgnoreStr string
}

var dbConfMap map[int]*dbCoreData

func init() {
	dbConfMap = make(map[int]*dbCoreData)
	dbConfMap[dbtype.MySQL] = &dbCoreData{
		DBType:   dbtype.MySQL,
		StrEsc:   "'",
		EscStart: "`",
		EscEnd:   "`",
		Esc:      "\\",
		BinStr:   "binary", // 数据库不区分大小写；放在数据之前，强制区分大小写
	}
	dbConfMap[dbtype.MariaDB] = &dbCoreData{
		DBType:   dbtype.MariaDB,
		StrEsc:   "'",
		EscStart: "`",
		EscEnd:   "`",
		Esc:      "\\",
		BinStr:   "binary", // 数据库不区分大小写；放在数据之前，强制区分大小写
	}
	dbConfMap[dbtype.SQLServer] = &dbCoreData{
		DBType:   dbtype.SQLServer,
		StrEsc:   "'",
		EscStart: "[",
		EscEnd:   "]",
		Esc:      "%",
		BinStr:   "COLLATE Chinese_PRC_CS_AS", // 数据库不区分大小写；放在操作符之前，强制区分大小写
	}
	dbConfMap[dbtype.Postgres] = &dbCoreData{
		DBType:    dbtype.Postgres,
		StrEsc:    "'",
		EscStart:  "\"",
		EscEnd:    "\"",
		Esc:       "\\",
		IgnoreStr: "", // 数据库本身区分大小写
	}
	dbConfMap[dbtype.OpenGauss] = &dbCoreData{
		DBType:    dbtype.OpenGauss,
		StrEsc:    "'",
		EscStart:  "\"",
		EscEnd:    "\"",
		Esc:       "\\",
		IgnoreStr: "", // 数据库本身区分大小写
	}
	dbConfMap[dbtype.SQLite2] = &dbCoreData{
		DBType:    dbtype.SQLite2,
		StrEsc:    "'",
		EscStart:  "\"",
		EscEnd:    "\"",
		Esc:       "",
		IgnoreStr: "COLLATE NOCASE", // 数据库本身区分大小写；放在操作符之前，忽略大小写
	}
	dbConfMap[dbtype.SQLite3] = &dbCoreData{
		DBType:    dbtype.SQLite3,
		StrEsc:    "'",
		EscStart:  "\"",
		EscEnd:    "\"",
		Esc:       "",
		IgnoreStr: "COLLATE NOCASE", // 数据库本身区分大小写，放在操作符之前，忽略大小写
	}
	dbConfMap[dbtype.Oracle] = &dbCoreData{
		DBType:    dbtype.Oracle,
		StrEsc:    "'",
		EscStart:  "\"",
		EscEnd:    "\"",
		Esc:       "",
		IgnoreStr: "", // 数据库本身区分大小写
	}
	dbConfMap[dbtype.ClickHouse] = &dbCoreData{
		DBType:   dbtype.ClickHouse,
		StrEsc:   "'",
		EscStart: "`",
		EscEnd:   "`",
		Esc:      "\\",
		BinStr:   "binary", // 数据库不区分大小写；放在数据之前，强制区分大小写
	}
}
