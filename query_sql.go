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
	if op == "date" {
		return fmt.Sprintf("'%s'", strings.Split(s, " ")[0])
	}
	return fmt.Sprintf("'%s'", s)
}

// query
func (p *queryModel) countDB() string {
	sql := "select "
	sel := p.selectSQL()
	if p.MainTable == "" {
		panic("MainTable is nil")
	}
	if p.MainAlias != "" {
		if p.DBCore.DBType == dbtype.Oracle {
			sql += sel + " from " + p.MainTable + " " + p.MainAlias
		} else {
			sql += sel + " from " + p.MainTable + " as " + p.MainAlias
		}
	} else {
		sql += sel + " from " + p.MainTable
	}

	join := p.joinSQL()
	if join != "" {
		sql += join
	}

	where := p.andSQL(p.Where)
	if where != "" {
		sql += " where " + where
	}

	if len(p.GroupBy) > 0 {
		sql += " group by " + util.JoinArr(p.GroupBy, ",")
	}

	if len(p.GroupBy) > 0 && len(p.Having) > 0 {
		sql += " having " + p.andSQL(p.Having)
	}

	if p.DBCore.DBType == dbtype.Oracle {
		sql = fmt.Sprintf("SELECT COUNT(*) as %sc%s from (%s) %scount_tb%s",
			p.DBCore.EscStart, p.DBCore.EscEnd,
			sql,
			p.DBCore.EscStart, p.DBCore.EscEnd)
	} else {
		sql = fmt.Sprintf("SELECT COUNT(*) as %sc%s from (%s) as %scount_tb%s",
			p.DBCore.EscStart, p.DBCore.EscEnd,
			sql,
			p.DBCore.EscStart, p.DBCore.EscEnd)
	}
	return sql
}

func (p *queryModel) innerFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch colOperator {
	case "eq":
		subSQL = colName + "=" + val
	case "between":
		subSQL = colName + " between " + val
	case "lt":
		subSQL = colName + "<" + val
	case "lte":
		subSQL = colName + "<=" + val
	case "gt":
		subSQL = colName + ">" + val
	case "gte":
		subSQL = colName + ">=" + val
	case "ne":
		subSQL = colName + "<>" + val
	case "in":
		subSQL = colName + " in " + val
	case "nin":
		subSQL = colName + " not in " + val
	case "date":
		if len(rawVal) == 10 {
			rawVal += " 00:00:00"
		}
		nextDay := time2Str(str2Time(rawVal).Add(time.Hour * 24))
		strDate := strings.Split(rawVal, " ")[0] + " 00:00:00"
		strNext := strings.Split(nextDay, " ")[0] + " 00:00:00"
		if p.DBCore.DBType == dbtype.Oracle {
			subSQL = fmt.Sprintf("%s>=%s and %s<%s",
				colName, oracleDateTime(strDate, false), colName, oracleDateTime(strNext, false))
		} else {
			subSQL = fmt.Sprintf("%s>='%s' and %s<'%s'",
				colName, strDate, colName, strNext)
		}
	case "null":
		if colData.(bool) {
			subSQL = colName + " is null"
		} else {
			subSQL = colName + " is not null"
		}
	default:
		subSQL = p.formatLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) innerBinFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch p.DBCore.DBType {
	case dbtype.MySQL, dbtype.MariaDB:
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
	case dbtype.MySQL, dbtype.MariaDB, dbtype.SQLServer:
		subSQL = p.innerFormatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
	}
	return subSQL
}

func (p *queryModel) formatLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL = colName + " like '" + rawVal + "%'"
		}
	case "endswith":
		if rawVal != "" {
			subSQL = colName + " like '%" + rawVal + "'"
		}
	case "contains":
		if rawVal != "" {
			subSQL = colName + " like '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like '%" + v + "%'"
				} else {
					subSQL += " and " + colName + " like '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "customlike":
		if rawVal != "" {
			subSQL = colName + " like '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like '" + v + "'"
				} else {
					subSQL += " and " + colName + " like '" + v + "'"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.formatOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) formatOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL = colName + " like '" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like '" + v + "%'"
				} else {
					subSQL += " or " + colName + " like '" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orendswith":
		if rawVal != "" {
			subSQL = colName + " like '%" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like '%" + v + "'"
				} else {
					subSQL += " or " + colName + " like '%" + v + "'"
				}
			}
			subSQL += ")"
		}
	case "orcontains":
		if rawVal != "" {
			subSQL = colName + " like '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like '%" + v + "%'"
				} else {
					subSQL += " or " + colName + " like '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL = colName + " like '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like '" + v + "'"
				} else {
					subSQL += " or " + colName + " like '" + v + "'"
				}
			}
			subSQL += ")"
		}
	default:
		panic("no definition")
	}
	return subSQL
}

// orm
func (orm *ORM) innerInsertSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()
	insertSQL := "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"

	if data == nil {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	var values []string

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
				values = append(values, fmt.Sprintf("%v", v))
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
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
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
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, colName, dbCore.EscEnd))
				values = append(values, val)
			}
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	insertSQL = fmt.Sprintf(insertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	return insertSQL, nil
}

func (orm *ORM) innerInsertManySQL(dataList []interface{}, cols []string) (string, error) {
	dbCore := orm.ref.getDBConf()

	var insertSQL strings.Builder
	insertSQL.WriteString("insert into ")
	insertSQL.WriteString(dbCore.EscStart)
	insertSQL.WriteString(orm.tableName)
	insertSQL.WriteString(dbCore.EscEnd)
	// insertSQL := "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s"

	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"

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
		for _, colName := range cols {
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := orm.formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal.WriteString("null,")
					continue
				}

				subVal.WriteString(val)
				subVal.WriteByte(',')
			} else if v, ok = valMap["#"+colName]; ok {
				subVal.WriteString(fmt.Sprintf("%v,", v))
			} else {
				subVal.WriteString("null,")
			}
		}
		s := []byte(subVal.String())
		s[len(s)-1] = ')'
		valArr = append(valArr, string(s))
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
		newCols = append(newCols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
	}
	insertSQL.WriteByte('(')
	insertSQL.WriteString(util.JoinArr(newCols, ","))
	insertSQL.WriteString(") values")
	insertSQL.WriteString(util.JoinArr(valArr, ","))
	// insertSQL = fmt.Sprintf(insertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	return insertSQL.String(), nil
}

func (orm *ORM) innerUpdateSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()
	updateSQL := "update " + dbCore.EscStart + "%s" + dbCore.EscEnd + " set %s where " +
		dbCore.EscStart + "%s" + dbCore.EscEnd + "=%s"

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

				upSet = append(upSet, fmt.Sprintf("%s%s%s=%v",
					dbCore.EscStart, k, dbCore.EscEnd, v))
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

			upSet = append(upSet, fmt.Sprintf("%s%s%s=%s",
				dbCore.EscStart, k, dbCore.EscEnd, val))
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

			upSet = append(upSet, fmt.Sprintf("%s%s%s=%s",
				dbCore.EscStart, colName, dbCore.EscEnd, val))
		}
	}
	if primaryVal == "" {
		return "", fmt.Errorf("sql primary value is empty, please check it")
	}

	primaryVal = strings.ReplaceAll(primaryVal, "'", dbCore.StrEsc+"'")
	updateSQL = fmt.Sprintf(updateSQL, orm.tableName, util.JoinArr(upSet, ","), orm.primaryKey, primaryVal)
	return updateSQL, nil
}

func (orm *ORM) innerReplaceSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()
	replaceSQL := "replace into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"

	if data == nil {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	var values []string

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
				values = append(values, fmt.Sprintf("%v", v))
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
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
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
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, colName, dbCore.EscEnd))
				values = append(values, val)
			}
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	replaceSQL = fmt.Sprintf(replaceSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	return replaceSQL, nil
}

func (orm *ORM) innerReplaceManySQL(dataList []interface{}, cols []string) (string, error) {
	dbCore := orm.ref.getDBConf()
	replaceSQL := "replace into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s"

	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of replace data is []map[string]interface{} or []*struct or []struct"

	var valArr []string
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

		subVal := "("
		for _, colName := range cols {
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := orm.formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal += "null,"
					continue
				}

				subVal += val + ","
			} else if v, ok = valMap["#"+colName]; ok {
				subVal += fmt.Sprintf("%v,", v)
			} else {
				subVal += "null,"
			}
		}
		subVal = subVal[:len(subVal)-1] + ")"
		valArr = append(valArr, subVal)
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("replace data is empty")
	}

	newCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		newCols = append(newCols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
	}
	replaceSQL = fmt.Sprintf(replaceSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	return replaceSQL, nil
}
