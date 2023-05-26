package orm

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/assembly-hub/basics/util"

	"github.com/assembly-hub/orm/dbtype"
)

func innerDateTime(s string, op string) string {
	var strBuff strings.Builder
	strBuff.Grow(2 + len(s))
	strBuff.WriteByte('\'')
	if op == "date" {
		strBuff.WriteString(strings.Split(s, " ")[0])
	} else {
		strBuff.WriteString(s)
	}
	strBuff.WriteByte('\'')
	return strBuff.String()
}

// query
func (p *queryModel) countDB() string {
	var sql strings.Builder
	sql.Grow(100)

	sql.WriteString("select ")
	sel := p.selectSQL()
	if p.MainTable == "" {
		panic("MainTable is nil")
	}
	if p.MainAlias != "" {
		if p.DBCore.DBType == dbtype.Oracle {
			sql.WriteString(sel)
			sql.WriteString(" from ")
			sql.WriteString(p.MainTable)
			sql.WriteByte(' ')
			sql.WriteString(p.MainAlias)
		} else {
			sql.WriteString(sel)
			sql.WriteString(" from ")
			sql.WriteString(p.MainTable)
			sql.WriteString(" as ")
			sql.WriteString(p.MainAlias)
		}
	} else {
		sql.WriteString(sel)
		sql.WriteString(" from ")
		sql.WriteString(p.MainTable)
	}

	join := p.joinSQL()
	if join != "" {
		sql.WriteString(join)
	}

	where := p.andSQL(p.Where)
	if where != "" {
		sql.WriteString(" where ")
		sql.WriteString(where)
	}

	if len(p.GroupBy) > 0 {
		sql.WriteString(" group by ")
		sql.WriteString(util.JoinArr(p.GroupBy, ","))
	}

	if len(p.GroupBy) > 0 && len(p.Having) > 0 {
		sql.WriteString(" having ")
		sql.WriteString(p.andSQL(p.Having))
	}

	rawSQL := sql.String()
	sql.Reset()
	sql.Grow(len(rawSQL) + 50)
	sql.WriteString("SELECT COUNT(*) as ")
	sql.WriteString(p.DBCore.EscStart)
	sql.WriteByte('c')
	sql.WriteString(p.DBCore.EscEnd)
	sql.WriteString(" from (")
	sql.WriteString(rawSQL)
	if p.DBCore.DBType == dbtype.Oracle {
		sql.WriteString(") ")
	} else {
		sql.WriteString(") as ")
	}
	sql.WriteString(p.DBCore.EscStart)
	sql.WriteString("count_tb")
	sql.WriteString(p.DBCore.EscEnd)
	return sql.String()
}

func (p *queryModel) innerFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	var subSQL strings.Builder
	subSQL.Grow(50)
	switch colOperator {
	case "eq":
		subSQL.WriteString(colName)
		subSQL.WriteByte('=')
		subSQL.WriteString(val)
	case "between":
		subSQL.WriteString(colName)
		subSQL.WriteString(" between ")
		subSQL.WriteString(val)
	case "lt":
		subSQL.WriteString(colName)
		subSQL.WriteByte('<')
		subSQL.WriteString(val)
	case "lte":
		subSQL.WriteString(colName)
		subSQL.WriteString("<=")
		subSQL.WriteString(val)
	case "gt":
		subSQL.WriteString(colName)
		subSQL.WriteByte('>')
		subSQL.WriteString(val)
	case "gte":
		subSQL.WriteString(colName)
		subSQL.WriteString(">=")
		subSQL.WriteString(val)
	case "ne":
		subSQL.WriteString(colName)
		subSQL.WriteString("<>")
		subSQL.WriteString(val)
	case "in":
		subSQL.WriteString(colName)
		subSQL.WriteString(" in ")
		subSQL.WriteString(val)
	case "nin":
		subSQL.WriteString(colName)
		subSQL.WriteString(" not in ")
		subSQL.WriteString(val)
	case "date":
		if len(rawVal) == 10 {
			rawVal += " 00:00:00"
		}
		nextDay := time2Str(str2Time(rawVal).Add(time.Hour * 24))
		strDate := strings.Split(rawVal, " ")[0] + " 00:00:00"
		strNext := strings.Split(nextDay, " ")[0] + " 00:00:00"
		if p.DBCore.DBType == dbtype.Oracle {
			subSQL.WriteString(colName)
			subSQL.WriteString(">=")
			subSQL.WriteString(oracleDateTime(strDate, false))
			subSQL.WriteString(" and ")
			subSQL.WriteString(colName)
			subSQL.WriteString("<")
			subSQL.WriteString(oracleDateTime(strNext, false))
		} else {
			subSQL.WriteString(colName)
			subSQL.WriteString(">='")
			subSQL.WriteString(strDate)
			subSQL.WriteString("' and ")
			subSQL.WriteString(colName)
			subSQL.WriteString("<'")
			subSQL.WriteString(strNext)
			subSQL.WriteByte('\'')
		}
	case "null":
		subSQL.WriteString(colName)
		if colData.(bool) {
			subSQL.WriteString(" is null")
		} else {
			subSQL.WriteString(" is not null")
		}
	default:
		return p.formatLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) innerBinFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch p.DBCore.DBType {
	case dbtype.MySQL, dbtype.MariaDB, dbtype.ClickHouse:
		subSQL = p.mysqlBinFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	case dbtype.SQLServer:
		subSQL = p.sqlserverBinFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	case dbtype.Oracle, dbtype.Postgres, dbtype.OpenGauss, dbtype.SQLite2, dbtype.SQLite3:
		return p.innerFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	}

	return subSQL
}

func (p *queryModel) innerIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch p.DBCore.DBType {
	case dbtype.SQLite2, dbtype.SQLite3:
		subSQL = p.sqliteIgnoreFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	case dbtype.Oracle:
		subSQL = p.oracleIgnoreFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	case dbtype.Postgres, dbtype.OpenGauss:
		subSQL = p.postgresIgnoreFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	case dbtype.MySQL, dbtype.MariaDB, dbtype.SQLServer, dbtype.ClickHouse:
		subSQL = p.innerFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	}
	return subSQL
}

func (p *queryModel) formatLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		}
	case "endswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteByte('\'')
		}
	case "contains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}

				subSQL.WriteString(colName)
				subSQL.WriteString(" like '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "customlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteByte('\'')
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteByte('\'')
			}
			subSQL.WriteByte(')')
		}
	default:
		return p.formatOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) formatOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteString("(")
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orendswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteString("(")
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" like '%")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	case "orcontains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteString("(")
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" like '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteByte('\'')
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteString("(")
				} else {
					subSQL.WriteString(" or ")
				}

				subSQL.WriteString(colName)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteByte('\'')
			}
			subSQL.WriteByte(')')
		}
	default:
		panic("no definition")
	}
	return subSQL.String()
}

// orm
func (orm *ORM) innerInsertOrReplaceSQL(tp string, data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()
	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	var insertSQL strings.Builder
	insertSQL.Grow(200)
	insertSQL.WriteString(tp)
	insertSQL.WriteString(" into ")
	insertSQL.WriteString(dbCore.EscStart)
	insertSQL.WriteString(orm.tableName)
	insertSQL.WriteString(dbCore.EscEnd)
	insertSQL.WriteByte('(')

	if data == nil {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of data is map[string]interface{} or *struct or struct"
	var cols []string
	var values []string

	switch data := data.(type) {
	case map[string]interface{}:
		cols = make([]string, 0, len(data))
		values = make([]string, 0, len(data))
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				var formatKey strings.Builder
				formatKey.Grow(escLen + len(k))
				formatKey.WriteString(dbCore.EscStart)
				formatKey.WriteString(k)
				formatKey.WriteString(dbCore.EscEnd)

				cols = append(cols, formatKey.String())
				values = append(values, util.InterfaceToString(v))
				continue
			}

			err := globalVerifyObj.VerifyFieldName(k)
			if err != nil {
				return "", err
			}

			val, timeEmpty := orm.formatValue(v)
			if k == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				var formatKey strings.Builder
				formatKey.Grow(escLen + len(k))
				formatKey.WriteString(dbCore.EscStart)
				formatKey.WriteString(k)
				formatKey.WriteString(dbCore.EscEnd)

				cols = append(cols, formatKey.String())
				values = append(values, val)
			}
		}
	default:
		dataValue := reflect.ValueOf(data)
		if dataValue.Type().Kind() != reflect.Struct && dataValue.Type().Kind() != reflect.Ptr {
			return "", fmt.Errorf(typeErrStr)
		}

		if dataValue.Type().Kind() == reflect.Ptr {
			dataValue = dataValue.Elem()
		}

		if dataValue.Type().Kind() != reflect.Struct {
			return "", fmt.Errorf(typeErrStr)
		}

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			err := globalVerifyObj.VerifyFieldName(colName)
			if err != nil {
				return "", err
			}

			val, timeEmpty := orm.formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				var formatKey strings.Builder
				formatKey.Grow(escLen + len(colName))
				formatKey.WriteString(dbCore.EscStart)
				formatKey.WriteString(colName)
				formatKey.WriteString(dbCore.EscEnd)

				cols = append(cols, formatKey.String())
				values = append(values, val)
			}
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	insertSQL.WriteString(util.JoinArr(cols, ","))
	insertSQL.WriteString(") values(")
	insertSQL.WriteString(util.JoinArr(values, ","))
	insertSQL.WriteByte(')')
	return insertSQL.String(), nil
}

func (orm *ORM) innerInsertOrReplaceManySQL(tp string, dataList []interface{}, cols []string) (string, error) {
	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	dbCore := orm.ref.getDBConf()

	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	var insertSQL strings.Builder
	insertSQL.Grow(len(dataList)*len(cols)*5 + 100)

	insertSQL.WriteString(tp)
	insertSQL.WriteString(" into ")
	insertSQL.WriteString(dbCore.EscStart)
	insertSQL.WriteString(orm.tableName)
	insertSQL.WriteString(dbCore.EscEnd)

	typeErrStr := "type of data is []map[string]interface{} or []*struct or []struct"

	valArr := make([]string, 0, len(dataList))
	for _, data := range dataList {
		var valMap map[string]interface{}
		switch data := data.(type) {
		case map[string]interface{}:
			valMap = data
		default:
			dataValue := reflect.ValueOf(data)
			if dataValue.Type().Kind() != reflect.Struct && dataValue.Type().Kind() != reflect.Ptr {
				return "", fmt.Errorf(typeErrStr)
			}

			if dataValue.Type().Kind() == reflect.Ptr {
				dataValue = dataValue.Elem()
			}

			if dataValue.Type().Kind() != reflect.Struct {
				return "", fmt.Errorf(typeErrStr)
			}

			valMap = map[string]interface{}{}

			for i := 0; i < dataValue.NumField(); i++ {
				colName := dataValue.Type().Field(i).Tag.Get("json")
				ref := dataValue.Type().Field(i).Tag.Get("ref")
				if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
					continue
				}

				valMap[colName] = dataValue.Field(i).Interface()
			}
		}
		if len(valMap) <= 0 {
			return "", fmt.Errorf("sql data is empty, please check it")
		}

		var subVal strings.Builder
		subVal.Grow(len(cols) * 10)
		subVal.WriteByte('(')
		for i := range cols {
			if i > 0 {
				subVal.WriteByte(',')
			}
			if v, ok := valMap[cols[i]]; ok {
				val, timeEmpty := orm.formatValue(v)
				if (cols[i] == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal.WriteString("null")
					continue
				}

				subVal.WriteString(val)
			} else if v, ok = valMap["#"+cols[i]]; ok {
				subVal.WriteString(util.InterfaceToString(v))
			} else {
				subVal.WriteString("null")
			}
		}
		subVal.WriteByte(')')
		valArr = append(valArr, subVal.String())
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("insert data is empty")
	}

	newCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}

		var buf strings.Builder
		buf.Grow(escLen + len(k))
		buf.WriteString(dbCore.EscStart)
		buf.WriteString(k)
		buf.WriteString(dbCore.EscEnd)

		newCols = append(newCols, buf.String())
	}
	insertSQL.WriteByte('(')
	insertSQL.WriteString(util.JoinArr(newCols, ","))
	insertSQL.WriteString(") values")
	insertSQL.WriteString(util.JoinArr(valArr, ","))
	return insertSQL.String(), nil
}

func (orm *ORM) innerUpdateSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()
	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	var updateSQL strings.Builder
	updateSQL.WriteString("update ")
	updateSQL.WriteString(dbCore.EscStart)
	updateSQL.WriteString(orm.tableName)
	updateSQL.WriteString(dbCore.EscEnd)
	updateSQL.WriteString(" set ")

	if data == nil {
		return "", fmt.Errorf("update data is nil")
	}

	typeErrStr := "type of update data is []map[string]interface{} or []*struct or []struct"
	var upSet []string

	primaryVal := ""

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}

				strVal := util.InterfaceToString(v)

				var formatSet strings.Builder
				formatSet.Grow(escLen + len(k) + len(strVal) + 1)
				formatSet.WriteString(dbCore.EscStart)
				formatSet.WriteString(k)
				formatSet.WriteString(dbCore.EscEnd)
				formatSet.WriteByte('=')
				formatSet.WriteString(strVal)
				upSet = append(upSet, formatSet.String())
				continue
			}

			err := globalVerifyObj.VerifyFieldName(k)
			if err != nil {
				return "", err
			}

			val, timeEmpty := orm.formatValue(v)
			if k == orm.primaryKey {
				primaryVal = val
				continue
			}
			if timeEmpty {
				val = "null"
			}

			var formatSet strings.Builder
			formatSet.Grow(escLen + len(k) + len(val) + 1)
			formatSet.WriteString(dbCore.EscStart)
			formatSet.WriteString(k)
			formatSet.WriteString(dbCore.EscEnd)
			formatSet.WriteByte('=')
			formatSet.WriteString(val)

			upSet = append(upSet, formatSet.String())
		}
	default:
		dataValue := reflect.ValueOf(data)
		if dataValue.Type().Kind() != reflect.Struct && dataValue.Type().Kind() != reflect.Ptr {
			return "", fmt.Errorf(typeErrStr)
		}

		if dataValue.Type().Kind() == reflect.Ptr {
			dataValue = dataValue.Elem()
		}

		if dataValue.Type().Kind() != reflect.Struct {
			return "", fmt.Errorf(typeErrStr)
		}

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			err := globalVerifyObj.VerifyFieldName(colName)
			if err != nil {
				return "", err
			}

			val, timeEmpty := orm.formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey {
				primaryVal = val
				continue
			}
			if timeEmpty {
				continue
			}

			var formatSet strings.Builder
			formatSet.Grow(escLen + len(colName) + len(val) + 1)
			formatSet.WriteString(dbCore.EscStart)
			formatSet.WriteString(colName)
			formatSet.WriteString(dbCore.EscEnd)
			formatSet.WriteByte('=')
			formatSet.WriteString(val)
			upSet = append(upSet, formatSet.String())
		}
	}
	if primaryVal == "" {
		return "", fmt.Errorf("sql primary value is empty, please check it")
	}

	primaryVal = strings.ReplaceAll(primaryVal, "'", "''")

	updateSQL.WriteString(util.JoinArr(upSet, ","))
	updateSQL.WriteString(" where ")
	updateSQL.WriteString(dbCore.EscStart)
	updateSQL.WriteString(orm.primaryKey)
	updateSQL.WriteString(dbCore.EscEnd)
	updateSQL.WriteByte('=')
	updateSQL.WriteString(primaryVal)
	return updateSQL.String(), nil
}
