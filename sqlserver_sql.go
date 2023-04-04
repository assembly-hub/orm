package orm

import (
	"fmt"
	"reflect"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

func (p *queryModel) querySQLServer() string {
	sql := "select "
	if len(p.Limit) == 1 {
		sql += " top(" + util.IntToStr(int64(p.Limit[0])) + ") "
	}
	sel := p.selectSQL()
	if p.MainTable == "" {
		panic("MainTable is nil")
	}
	if p.MainAlias != "" {
		sql += sel + " from " + p.MainTable + " as " + p.MainAlias
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

	order := p.orderSQL()
	orderSQL := ""
	if order != "" {
		sql += " order by " + order
	} else {
		orderSQL = " order by " + p.DBCore.EscStart + p.PrivateKey + p.DBCore.EscEnd
	}

	if len(p.Limit) == 2 {
		sql += fmt.Sprintf("%s offset %d rows fetch next %d rows only", orderSQL, p.Limit[0], p.Limit[1])
	}

	// SelectMethod=cursor
	if p.SelectForUpdate {
		sql += " for update"
	}

	return sql
}

func (p *queryModel) sqlserverBinFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch colOperator {
	case "eq":
		subSQL = colName + " " + p.DBCore.BinStr + " =" + val
	case "lt":
		subSQL = colName + " " + p.DBCore.BinStr + " <" + val
	case "lte":
		subSQL = colName + " " + p.DBCore.BinStr + " <=" + val
	case "gt":
		subSQL = colName + " " + p.DBCore.BinStr + " >" + val
	case "gte":
		subSQL = colName + " " + p.DBCore.BinStr + " >=" + val
	case "ne":
		subSQL = colName + " " + p.DBCore.BinStr + " <>" + val
	case "in":
		subSQL = colName + " " + p.DBCore.BinStr + " in " + val
	case "nin":
		subSQL = colName + " " + p.DBCore.BinStr + " not in " + val
	default:
		subSQL = p.sqlserverBinLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) sqlserverBinLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '" + rawVal + "%'"
		}
	case "endswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '%" + rawVal + "'"
		}
	case "contains":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.BinStr + " like '%" + v + "%'"
				} else {
					subSQL += " and " + colName + " " + p.DBCore.BinStr + " like '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "customlike":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.BinStr + " like '" + v + "'"
				} else {
					subSQL += " and " + colName + " " + p.DBCore.BinStr + " like '" + v + "'"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.sqlserverBinOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) sqlserverBinOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.BinStr + " like '" + v + "%'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.BinStr + " like '" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orendswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '%" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.BinStr + " like '%" + v + "'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.BinStr + " like '%" + v + "'"
				}
			}
			subSQL += ")"
		}
	case "orcontains":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.BinStr + " like '%" + v + "%'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.BinStr + " like '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.BinStr + " like '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.BinStr + " like '" + v + "'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.BinStr + " like '" + v + "'"
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
func (orm *ORM) sqlserverUpsertSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()

	if data == nil {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	var realCol []string
	colSet := set.New[string]()
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
				formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd)
				cols = append(cols, formatKey)
				colSet.Add(k)
				values = append(values, fmt.Sprintf("%v", v))

				if orm.sqlserverExcludePK && k == orm.primaryKey {
					continue
				}
				realCol = append(realCol, formatKey)
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
			values = append(values, val)

			if orm.sqlserverExcludePK && k == orm.primaryKey {
				continue
			}
			realCol = append(realCol, formatKey)
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
			values = append(values, val)

			if orm.sqlserverExcludePK && colName == orm.primaryKey {
				continue
			}
			realCol = append(realCol, formatKey)
		}
	}
	if len(realCol) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	var update []string

	upsertSQL := "MERGE INTO " + dbCore.EscStart + "%s" + dbCore.EscEnd + " as [T] " +
		"USING (values%s) AS [S](%s) ON (%s) WHEN MATCHED THEN " +
		"UPDATE SET %s " +
		"WHEN NOT MATCHED THEN " +
		"INSERT(%s) VALUES(%s);"
	if hasPK {
		for i := range realCol {
			update = append(update, fmt.Sprintf("[T].%s=[S].%s", realCol[i], realCol[i]))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, "("+util.JoinArr(values, ",")+")", util.JoinArr(cols, ","),
			fmt.Sprintf("[T].%s%s%s=[S].%s%s%s",
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd,
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd),
			util.JoinArr(update, ","), util.JoinArr(realCol, ","), connectStrArr(realCol, ",", "[S].", ""))
	} else if hasUK {
		var onList []string
		orm.uniqueKeys.Range(func(item string) bool {
			onList = append(onList, fmt.Sprintf("[T].%s%s%s=[S].%s%s%s",
				dbCore.EscStart, item, dbCore.EscEnd,
				dbCore.EscStart, item, dbCore.EscEnd))
			return true
		})
		for i := range realCol {
			k := realCol[i]
			if orm.uniqueKeys.Has(k[len(dbCore.EscStart) : len(k)-len(dbCore.EscEnd)]) {
				continue
			}
			update = append(update, fmt.Sprintf("[T].%s=[S].%s", k, k))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, "("+util.JoinArr(values, ",")+")", util.JoinArr(cols, ","),
			util.JoinArr(onList, " and "),
			util.JoinArr(update, ","), util.JoinArr(realCol, ","), connectStrArr(realCol, ",", "[S].", ""))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	}

	return upsertSQL, nil
}

func (orm *ORM) sqlserverUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
	dbCore := orm.ref.getDBConf()

	if len(dataList) <= 0 {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"

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
		return "", fmt.Errorf("upsert data is empty")
	}

	newCols := make([]string, 0, len(cols))
	var realCol []string

	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		formatKey := fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd)
		newCols = append(newCols, formatKey)
		if orm.sqlserverExcludePK && k == orm.primaryKey {
			continue
		}
		realCol = append(realCol, formatKey)
	}

	colSet := set.New[string]()
	colSet.Add(cols...)

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	updateCols := make([]string, 0, len(realCol))

	upsertSQL := "MERGE INTO " + dbCore.EscStart + "%s" + dbCore.EscEnd + " as [T] " +
		"USING (values%s) AS [S](%s) ON (%s) WHEN MATCHED THEN " +
		"UPDATE SET %s " +
		"WHEN NOT MATCHED THEN " +
		"INSERT(%s) VALUES(%s);"
	if hasPK {
		for i := range realCol {
			updateCols = append(updateCols, fmt.Sprintf("[T].%s=[S].%s", realCol[i], realCol[i]))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(valArr, ","), util.JoinArr(cols, ","),
			fmt.Sprintf("[T].%s%s%s=[S].%s%s%s",
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd,
				dbCore.EscStart, orm.primaryKey, dbCore.EscEnd),
			util.JoinArr(updateCols, ","), util.JoinArr(realCol, ","), connectStrArr(realCol, ",", "[S].", ""))
	} else if hasUK {
		var onList []string
		orm.uniqueKeys.Range(func(item string) bool {
			onList = append(onList, fmt.Sprintf("[T].%s%s%s=[S].%s%s%s",
				dbCore.EscStart, item, dbCore.EscEnd,
				dbCore.EscStart, item, dbCore.EscEnd))
			return true
		})
		for i := range realCol {
			k := realCol[i]
			if orm.uniqueKeys.Has(k[len(dbCore.EscStart) : len(k)-len(dbCore.EscEnd)]) {
				continue
			}
			updateCols = append(updateCols, fmt.Sprintf("[T].%s=[S].%s", k, k))
		}
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(valArr, ","), util.JoinArr(cols, ","),
			util.JoinArr(onList, " and "),
			util.JoinArr(updateCols, ","), util.JoinArr(realCol, ","), connectStrArr(realCol, ",", "[S].", ""))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	}

	return upsertSQL, nil
}
