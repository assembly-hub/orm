package orm

import (
	"fmt"
	"reflect"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

const oracleTimeFormat = "'yyyy-mm-dd hh24:mi:ss'"
const oracleDateFormat = "'yyyy-mm-dd'"

func oracleDateTime(s string, isDate bool) string {
	if isDate {
		return fmt.Sprintf("TO_DATE('%s',%s)", s, oracleDateFormat)
	}
	return fmt.Sprintf("TO_DATE('%s',%s)", s, oracleTimeFormat)
}

// queryModel
func (p *queryModel) queryOracle() string {
	sql := "select "
	sel := p.selectSQL()
	if p.MainTable == "" {
		panic("MainTable is nil")
	}
	if p.MainAlias != "" {
		sql += sel + " from " + p.MainTable + " " + p.MainAlias
	} else {
		sql += sel + " from " + p.MainTable
	}

	join := p.joinSQL()
	if join != "" {
		sql += join
	}

	limitSQL := ""
	if len(p.Limit) == 1 {
		sql += "rownum<=" + util.IntToStr(int64(p.Limit[0]))
	} else if len(p.Limit) == 2 {
		sql += fmt.Sprintf("rownum>%d and rownum<=%d", p.Limit[0], p.Limit[0]+p.Limit[1])
	}

	where := p.andSQL(p.Where)
	if where != "" {
		sql += " where " + where + " and " + limitSQL
	} else {
		sql += " where " + limitSQL
	}

	if len(p.GroupBy) > 0 {
		sql += " group by " + util.JoinArr(p.GroupBy, ",")
	}

	if len(p.GroupBy) > 0 && len(p.Having) > 0 {
		sql += " having " + p.andSQL(p.Having)
	}

	order := p.orderSQL()
	if order != "" {
		sql += " order by " + order
	}

	if p.SelectForUpdate {
		sql += " for update"
	}

	return sql
}

func (p *queryModel) oracleIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	colName = "LOWER(" + colName + ")"
	val = "LOWER(" + val + ")"
	switch colOperator {
	case "eq":
		subSQL = colName + "=" + val
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
		newVal := "(" + connectStrArr(rawStrArr, ",", "LOWER('", "')") + ")"
		subSQL = colName + " in " + newVal
	case "nin":
		newVal := "(" + connectStrArr(rawStrArr, ",", "LOWER('", "')") + ")"
		subSQL = colName + " not in " + newVal
	default:
		subSQL = p.oracleIgnoreLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) oracleIgnoreLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL = colName + " like LOWER('" + rawVal + "%')"
		}
	case "endswith":
		if rawVal != "" {
			subSQL = colName + " like LOWER('%" + rawVal + "')"
		}
	case "contains":
		if rawVal != "" {
			subSQL = colName + " like LOWER('%" + rawVal + "%')"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like LOWER('%" + v + "%')"
				} else {
					subSQL += " and " + colName + " like LOWER('%" + v + "%')"
				}
			}
			subSQL += ")"
		}
	case "customlike":
		if rawVal != "" {
			subSQL = colName + " like LOWER('" + rawVal + "')"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like LOWER('" + v + "')"
				} else {
					subSQL += " and " + colName + " like LOWER('" + v + "')"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.oracleIgnoreOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) oracleIgnoreOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL = colName + " like LOWER('" + rawVal + "%')"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like LOWER('" + v + "%')"
				} else {
					subSQL += " or " + colName + " like LOWER('" + v + "%')"
				}
			}
			subSQL += ")"
		}
	case "orendswith":
		if rawVal != "" {
			subSQL = colName + " like LOWER('%" + rawVal + "')"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like LOWER('%" + v + "')"
				} else {
					subSQL += " or " + colName + " like LOWER('%" + v + "')"
				}
			}
			subSQL += ")"
		}
	case "orcontains":
		if rawVal != "" {
			subSQL = colName + " like LOWER('%" + rawVal + "%')"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like LOWER('%" + v + "%')"
				} else {
					subSQL += " or " + colName + " like LOWER('%" + v + "%')"
				}
			}
			subSQL += ")"
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL = colName + " like LOWER('" + rawVal + "')"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like LOWER('" + v + "')"
				} else {
					subSQL += " or " + colName + " like LOWER('" + v + "')"
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
func (orm *ORM) oracleInsertManySQL(dataList []interface{}, cols []string) (string, error) {
	dbCore := orm.ref.getDBConf()
	insertSQL := "insert all into "

	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	newCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		newCols = append(newCols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
	}

	tableAndField := dbCore.EscStart + orm.tableName + dbCore.EscEnd + "(" + util.JoinArr(newCols, ",") + ")"
	insertSQL += tableAndField

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"

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

		subVal := ""
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
		subVal = subVal[:len(subVal)-1]
		valArr = append(valArr, tableAndField)
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("insert data is empty")
	}

	dataSql := ""
	for i := 0; i < len(valArr)-1; i++ {
		dataSql += fmt.Sprintf(" into %s VALUES(%s)", tableAndField, valArr[i])
	}

	insertSQL += dataSql + " select " + valArr[len(valArr)-1] + " from \"DUAL\""
	return insertSQL, nil
}

func (orm *ORM) oracleUpsertSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()

	if data == nil {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	colSet := set.New[string]()
	var values []string
	var rawVal []string

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd)
				cols = append(cols, formatKey)
				colSet.Add(k)
				values = append(values, fmt.Sprintf("%v as %s", v, formatKey))
				rawVal = append(rawVal, fmt.Sprintf("%v", v))
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
			}
			formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd)
			cols = append(cols, formatKey)
			colSet.Add(k)
			values = append(values, fmt.Sprintf("%s as %s", val, formatKey))
			rawVal = append(rawVal, fmt.Sprintf("%s", val))
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

			val, timeEmpty := orm.formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			}
			formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, colName, dbCore.EscEnd)
			cols = append(cols, formatKey)
			colSet.Add(colName)
			values = append(values, fmt.Sprintf("%s as %s", val, formatKey))
			rawVal = append(rawVal, fmt.Sprintf("%s", val))
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	var update []string

	upsertSQL := "MERGE INTO " + dbCore.EscStart + "%s" + dbCore.EscEnd + " \"T\" " +
		"USING (%s) \"S\" ON (%s) WHEN MATCHED THEN " +
		"UPDATE SET %s " +
		"WHEN NOT MATCHED THEN " +
		"INSERT(%s) VALUES(%s)"
	if hasPK {
		for i := range cols {
			if dbCore.EscStart+orm.primaryKey+dbCore.EscEnd == cols[i] {
				continue
			}
			update = append(update, fmt.Sprintf("\"T\".%s=\"S\".%s", cols[i], cols[i]))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, "SELECT "+util.JoinArr(values, ",")+" from \"DUAL\"",
			fmt.Sprintf("\"T\".%s%s%s=\"S\".%s%s%s",
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd,
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd),
			util.JoinArr(update, ","), util.JoinArr(cols, ","), connectStrArr(cols, ",", "\"S\".", ""))
	} else if hasUK {
		var onList []string
		orm.uniqueKeys.Range(func(item string) bool {
			onList = append(onList, fmt.Sprintf("\"T\".%s%s%s=\"S\".%s%s%s",
				dbCore.EscStart, item, dbCore.EscEnd,
				dbCore.EscStart, item, dbCore.EscEnd))
			return true
		})
		for i := range cols {
			k := cols[i]
			if orm.uniqueKeys.Has(k[len(dbCore.EscStart) : len(k)-len(dbCore.EscEnd)]) {
				continue
			}
			update = append(update, fmt.Sprintf("\"T\".%s=\"S\".%s", k, k))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, "SELECT "+util.JoinArr(values, ",")+" from \"DUAL\"",
			util.JoinArr(onList, " and "),
			util.JoinArr(update, ","), util.JoinArr(cols, ","), connectStrArr(cols, ",", "\"S\".", ""))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(rawVal, ","))
	}

	return upsertSQL, nil
}

func (orm *ORM) oracleUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
	dbCore := orm.ref.getDBConf()

	if len(dataList) <= 0 {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"

	var valArr []string
	var rawVal []string
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

		subVal := "SELECT "
		subStr := "("
		for _, colName := range cols {
			formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, colName, dbCore.EscEnd)
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := orm.formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal += "null as " + formatKey + ","
					subStr += "null,"
					continue
				}

				subVal += val + " as " + formatKey + ","
				subStr += val + ","
			} else if v, ok = valMap["#"+colName]; ok {
				subVal += fmt.Sprintf("%v as %s,", v, formatKey)
				subStr += fmt.Sprintf("%v", v)
			} else {
				subVal += "null as " + formatKey + ","
				subStr += "null,"
			}
		}
		subVal = subVal[:len(subVal)-1] + " from \"DUAL\""
		subStr = subStr[:len(subStr)-1] + ")"
		valArr = append(valArr, subVal)
		rawVal = append(rawVal, subStr)
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("upsert data is empty")
	}

	newCols := make([]string, 0, len(cols))

	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd)
		newCols = append(newCols, formatKey)
	}

	colSet := set.New[string]()
	colSet.Add(cols...)

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	updateCols := make([]string, 0, len(newCols))

	upsertSQL := "MERGE INTO " + dbCore.EscStart + "%s" + dbCore.EscEnd + " \"T\" " +
		"USING (%s) \"S\" ON (%s) WHEN MATCHED THEN " +
		"UPDATE SET %s " +
		"WHEN NOT MATCHED THEN " +
		"INSERT(%s) VALUES(%s)"
	unionStr := " UNION "
	if orm.oracleMergeUnionAll {
		unionStr = " UNION ALL "
	}
	if hasPK {
		for i := range newCols {
			if dbCore.EscStart+orm.primaryKey+dbCore.EscEnd == newCols[i] {
				continue
			}
			updateCols = append(updateCols, fmt.Sprintf("\"T\".%s=\"S\".%s", newCols[i], newCols[i]))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(valArr, unionStr),
			fmt.Sprintf("\"T\".%s%s%s=\"S\".%s%s%s",
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd,
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd),
			util.JoinArr(updateCols, ","), util.JoinArr(newCols, ","), connectStrArr(newCols, ",", "\"S\".", ""))
	} else if hasUK {
		var onList []string
		orm.uniqueKeys.Range(func(item string) bool {
			onList = append(onList, fmt.Sprintf("\"T\".%s%s%s=\"S\".%s%s%s",
				dbCore.EscStart, item, dbCore.EscEnd,
				dbCore.EscStart, item, dbCore.EscEnd))
			return true
		})
		for i := range newCols {
			k := newCols[i]
			if orm.uniqueKeys.Has(k[len(dbCore.EscStart) : len(k)-len(dbCore.EscEnd)]) {
				continue
			}
			updateCols = append(updateCols, fmt.Sprintf("\"T\".%s=\"S\".%s", k, k))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(valArr, unionStr),
			util.JoinArr(onList, " and "),
			util.JoinArr(updateCols, ","), util.JoinArr(newCols, ","), connectStrArr(newCols, ",", "\"S\".", ""))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(rawVal, ","))
	}

	return upsertSQL, nil
}
