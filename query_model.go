// Package orm
package orm

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/assembly-hub/basics/util"

	"github.com/assembly-hub/orm/dbtype"
)

type selectModel struct {
	Table string
	Cols  []string
}

type orderModel struct {
	Table string
	Cols  []string
}

type joinModel struct {
	Type      joinType
	MainTable string
	MainAlias string
	JoinTable string
	JoinAlias string
	On        [][2]string
}

type queryModel struct {
	PrivateKey      string
	DBCore          *dbCoreData
	MainTable       string
	MainAlias       string
	Distinct        bool
	SelectForUpdate bool
	Select          []*selectModel
	Order           []*orderModel
	Limit           []uint
	Where           map[string]interface{}
	JoinList        []*joinModel
	GroupBy         []string
	Having          map[string]interface{}
}

func (p *queryModel) selectSQL() string {
	var sqlBuff strings.Builder
	sqlBuff.Grow(200)
	for _, sel := range p.Select {
		for _, col := range sel.Cols {
			if sel.Table != "" {
				col = sel.Table + "." + col
			}
			if sqlBuff.Len() <= 0 {
				sqlBuff.WriteString(col)
			} else {
				sqlBuff.WriteByte(',')
				sqlBuff.WriteString(col)
			}
		}
	}

	if sqlBuff.Len() <= 0 {
		sqlBuff.WriteByte('*')
	}

	if p.Distinct {
		sql := sqlBuff.String()
		sqlBuff.Reset()
		sqlBuff.Grow(len(sql) + 9)
		sqlBuff.WriteString("distinct ")
		sqlBuff.WriteString(sql)
	}
	return sqlBuff.String()
}

func (p *queryModel) joinSQL() string {
	var sqlBuff, whereBuff strings.Builder
	sqlBuff.Grow(150)
	for _, join := range p.JoinList {
		joinStr := ""
		if join.Type.value() != "" {
			joinStr = join.Type.value() + " "
		}
		if join.JoinAlias != "" {
			if p.DBCore.DBType == dbtype.Oracle {
				sqlBuff.WriteByte(' ')
				sqlBuff.WriteString(joinStr)
				sqlBuff.WriteString("join ")
				sqlBuff.WriteString(join.JoinTable)
				sqlBuff.WriteByte(' ')
				sqlBuff.WriteString(join.JoinAlias)
			} else {
				sqlBuff.WriteByte(' ')
				sqlBuff.WriteString(joinStr)
				sqlBuff.WriteString("join ")
				sqlBuff.WriteString(join.JoinTable)
				sqlBuff.WriteString(" as ")
				sqlBuff.WriteString(join.JoinAlias)
			}
		} else {
			sqlBuff.WriteByte(' ')
			sqlBuff.WriteString(joinStr)
			sqlBuff.WriteString("join ")
			sqlBuff.WriteString(join.JoinTable)
		}

		whereBuff.Reset()
		whereBuff.Grow(50)
		mt := join.MainTable
		if join.MainAlias != "" {
			mt = join.MainAlias
		}
		jt := join.JoinTable
		if join.JoinAlias != "" {
			jt = join.JoinAlias
		}
		for _, sel := range join.On {
			if whereBuff.Len() > 0 {
				whereBuff.WriteString(" and ")
			}

			whereBuff.WriteString(mt)
			whereBuff.WriteByte('.')
			whereBuff.WriteString(sel[0])
			whereBuff.WriteByte('=')
			whereBuff.WriteString(jt)
			whereBuff.WriteByte('.')
			whereBuff.WriteString(sel[1])
		}
		if whereBuff.Len() > 0 {
			sqlBuff.WriteString(" on ")
			sqlBuff.WriteString(whereBuff.String())
		}
	}

	return sqlBuff.String()
}

func (p *queryModel) orderSQL() string {
	var sql strings.Builder
	sql.Grow(50)
	for _, sel := range p.Order {
		for _, col := range sel.Cols {
			if col[0] == '-' {
				col = col[1:] + " desc"
			} else if col[0] == '+' {
				col = col[1:] + " asc"
			} else {
				col = col + " asc"
			}

			if sel.Table != "" {
				col = sel.Table + "." + col
			}
			if sql.Len() > 0 {
				sql.WriteByte(',')
			}

			sql.WriteString(col)
		}
	}

	return sql.String()
}

func (p *queryModel) orSQL(where map[string]interface{}) string {
	return p.whereSQL(where, " or ")
}

func (p *queryModel) andSQL(where map[string]interface{}) string {
	return p.whereSQL(where, " and ")
}

func (p *queryModel) formatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""

	opArr := strings.Split(colOperator, "_")

	if len(opArr) == 0 {
		panic(fmt.Errorf("operator[] cannot be empty"))
	}

	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Sprintf("field[%s]'s operator[%s] %v", colName, colOperator, e))
		}
	}()

	if len(opArr) == 1 {
		subSQL = p.innerFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	} else if opArr[0] == "bin" || opArr[0] == "b" {
		subSQL = p.innerBinFormatSubSQL(opArr[1], colName, val, rawVal, rawStrArr, colData)
	} else if opArr[0] == "ignore" || opArr[0] == "i" {
		subSQL = p.innerIgnoreFormatSubSQL(opArr[1], colName, val, rawVal, rawStrArr, colData)
	} else {
		panic(fmt.Errorf("operator[%s] is not supported", colOperator))
	}

	return subSQL
}

func (p *queryModel) formatTimeValue(colOperator, colName string, colData interface{}) (val string, rawVal string, rawStrArr []string) {
	switch colData := colData.(type) {
	case time.Time, *time.Time:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		rawVal = time2Str(colData)
		if p.DBCore.DBType == dbtype.Oracle {
			val = oracleDateTime(rawVal, false)
		} else {
			val = innerDateTime(rawVal, colOperator)
		}
	case []time.Time:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic(ErrBetweenValueMatch)
			}
			if p.DBCore.DBType == dbtype.Oracle {
				val = oracleDateTime(time2Str(colData[0]), false) + " and " +
					oracleDateTime(time2Str(colData[1]), false)
			} else {
				val = "'" + time2Str(colData[0]) + "' and '" + time2Str(colData[1]) + "'"
			}
		} else {
			for _, v := range colData {
				if p.DBCore.DBType == dbtype.Oracle {
					rawStrArr = append(rawStrArr, oracleDateTime(time2Str(v), false))
				} else {
					rawStrArr = append(rawStrArr, "'"+time2Str(v)+"'")
				}
			}
			val = "(" + util.JoinArr[string](rawStrArr, ",") + ")"
		}
	case []*time.Time:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic(ErrBetweenValueMatch)
			}
			if p.DBCore.DBType == dbtype.Oracle {
				val = oracleDateTime(time2Str(colData[0]), false) + " and " +
					oracleDateTime(time2Str(colData[1]), false)
			} else {
				val = "'" + time2Str(colData[0]) + "' and '" + time2Str(colData[1]) + "'"
			}
		} else {
			for _, v := range colData {
				if p.DBCore.DBType == dbtype.Oracle {
					rawStrArr = append(rawStrArr, oracleDateTime(time2Str(v), false))
				} else {
					rawStrArr = append(rawStrArr, "'"+time2Str(v)+"'")
				}
			}
			val = "(" + util.JoinArr[string](rawStrArr, ",") + ")"
		}
	}
	return
}

func (p *queryModel) formatSQLValue(colOperator, colName string, colData interface{}) (val string, rawVal string, rawStrArr []string) {
	switch colData := colData.(type) {
	case queryModel:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		rawVal = colData.SQL()
		if rawVal != "" {
			val = "(" + rawVal + ")"
		}
	case *queryModel:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		rawVal = colData.SQL()
		if rawVal != "" {
			val = "(" + rawVal + ")"
		}
	case string:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		rawVal = strings.ReplaceAll(colData, "'", "''")
		val = "'" + rawVal + "'"
	case []string:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic(ErrBetweenValueMatch)
			}
			val = "'" + strings.ReplaceAll(colData[0], "'", "''") + "' and '" +
				strings.ReplaceAll(colData[1], "'", "''") + "'"
		} else {
			for _, v := range colData {
				v = strings.ReplaceAll(v, "'", "''")
				rawStrArr = append(rawStrArr, v)
			}
			val = "(" + connectStrArr(rawStrArr, ",", "'", "'") + ")"
		}
	case []interface{}:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic(ErrBetweenValueMatch)
			}
			val = "'" + strings.ReplaceAll(fmt.Sprintf("%v", colData[0]), "'", "''") + "' and '" +
				strings.ReplaceAll(fmt.Sprintf("%v", colData[1]), "'", "''") + "'"
		} else {
			for _, vv := range colData {
				v := fmt.Sprintf("%v", vv)
				v = strings.ReplaceAll(v, "'", "''")
				rawStrArr = append(rawStrArr, v)
			}
			val = "(" + connectStrArr(rawStrArr, ",", "'", "'") + ")"
		}
	case bool:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		if colData {
			val = "1"
		} else {
			val = "0"
		}
		rawVal = val
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		val = fmt.Sprintf("%v", colData)
		rawVal = val
	case []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64:
		slice := reflect.ValueOf(colData)
		if slice.Len() <= 0 {
			panic(fmt.Sprintf("colName:[%s] slice not empty", colName))
		}

		if colOperator == "between" {
			if slice.Len() != 2 {
				panic(ErrBetweenValueMatch)
			}
			val = fmt.Sprintf("%v", slice.Index(0).Interface()) + " and " +
				fmt.Sprintf("%v", slice.Index(1).Interface())
		} else {
			val = "("
			for i := 0; i < slice.Len(); i++ {
				val += fmt.Sprintf("%v", slice.Index(i).Interface()) + ","
			}

			val = val[0:len(val)-1] + ")"
		}
	case time.Time, *time.Time, []time.Time, []*time.Time:
		val, rawVal, rawStrArr = p.formatTimeValue(colOperator, colName, colData)
	default:
		if colOperator == "between" {
			panic(ErrBetweenValueMatch)
		}

		rawVal = strings.ReplaceAll(fmt.Sprintf("%v", colData), "'", "''")
		val = "'" + rawVal + "'"
	}

	return
}

func (p *queryModel) whereSQL(where map[string]interface{}, linker string) string {
	sql := ""
	for colKey, colData := range where {
		if colKey == "" || colData == nil {
			continue
		}

		not := false
		if colKey[:1] == "~" {
			not = true
			colKey = colKey[1:]
		}

		subSQL := ""
		if colKey == "$or" {
			switch colData := colData.(type) {
			case map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				subSQL = p.orSQL(colData)
				if subSQL != "" {
					subSQL = "(" + subSQL + ")"
				}
			case []map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				var sqlArr []string
				for _, d := range colData {
					if len(d) <= 0 {
						continue
					}

					strSQL := p.andSQL(d)
					if strSQL != "" {
						sqlArr = append(sqlArr, "("+strSQL+")")
					}
				}
				if len(sqlArr) > 0 {
					subSQL = "(" + util.JoinArr(sqlArr, " or ") + ")"
				}
			default:
				panic("or cond error")
			}
		} else if colKey == "$and" {
			switch colData := colData.(type) {
			case map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				subSQL = p.andSQL(colData)
				if subSQL != "" {
					subSQL = "(" + subSQL + ")"
				}
			case []map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				var sqlArr []string
				for _, d := range colData {
					if len(d) <= 0 {
						continue
					}

					strSQL := p.andSQL(d)
					if strSQL != "" {
						sqlArr = append(sqlArr, "("+strSQL+")")
					}
				}
				if len(sqlArr) > 0 {
					subSQL = "(" + util.JoinArr(sqlArr, " and ") + ")"
				}
			default:
				panic("and cond error")
			}
		} else {
			i := strings.Index(colKey, "__")
			colName, colOperator := "", ""
			if i < 0 {
				colName = colKey
				colOperator = "eq"
			} else {
				arr := strings.Split(colKey, "__")
				colName = arr[0]
				colOperator = arr[1]
			}

			val, rawVal, rawStrArr := p.formatSQLValue(colOperator, colName, colData)
			subSQL = p.formatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
		}

		if subSQL == "" {
			continue
		}

		if not {
			if subSQL[:1] == "(" {
				subSQL = "(not " + subSQL + ")"
			} else {
				subSQL = "(not (" + subSQL + "))"
			}
		}

		if sql == "" {
			sql = subSQL
		} else {
			sql += linker + subSQL
		}
	}
	return sql
}

func (p *queryModel) GetWhere() string {
	where := p.andSQL(p.Where)
	return where
}

func (p *queryModel) Count() string {
	return p.countDB()
}

func (p *queryModel) SQL() string {
	switch p.DBCore.DBType {
	case dbtype.MySQL, dbtype.MariaDB, dbtype.OpenGauss,
		dbtype.Postgres, dbtype.ClickHouse:
		return p.queryMySQL()
	case dbtype.SQLite2, dbtype.SQLite3:
		return p.querySQLite()
	case dbtype.SQLServer:
		return p.querySQLServer()
	case dbtype.Oracle:
		return p.queryOracle()
	}
	return ""
}
