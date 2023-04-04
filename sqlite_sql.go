package orm

import (
	"fmt"
	"reflect"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

func (p *queryModel) querySQLite() string {
	sql := "select "
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
	if order != "" {
		sql += " order by " + order
	}

	if len(p.Limit) == 1 {
		sql += " limit " + util.IntToStr(int64(p.Limit[0]))
	} else if len(p.Limit) == 2 {
		sql += fmt.Sprintf(" limit %d offset %d", p.Limit[1], p.Limit[0])
	}

	//if p.SelectForUpdate {
	//	sql += " for update"
	//}

	return sql
}

func (p *queryModel) sqliteIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch colOperator {
	case "eq":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " =" + val
	case "lt":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " <" + val
	case "lte":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " <=" + val
	case "gt":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " >" + val
	case "gte":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " >=" + val
	case "ne":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " <>" + val
	case "in":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " in " + val
	case "nin":
		subSQL = colName + " " + p.DBCore.IgnoreStr + " not in " + val
	default:
		subSQL = p.sqliteIgnoreLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) sqliteIgnoreLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '" + rawVal + "%'"
		}
	case "endswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '%" + rawVal + "'"
		}
	case "contains":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.IgnoreStr + " like '%" + v + "%'"
				} else {
					subSQL += " and " + colName + " " + p.DBCore.IgnoreStr + " like '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "customlike":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.IgnoreStr + " like '" + v + "'"
				} else {
					subSQL += " and " + colName + " " + p.DBCore.IgnoreStr + " like '" + v + "'"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.sqliteIgnoreOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) sqliteIgnoreOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.IgnoreStr + " like '" + v + "%'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.IgnoreStr + " like '" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orendswith":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '%" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.IgnoreStr + " like '%" + v + "'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.IgnoreStr + " like '%" + v + "'"
				}
			}
			subSQL += ")"
		}
	case "orcontains":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.IgnoreStr + " like '%" + v + "%'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.IgnoreStr + " like '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL = colName + " " + p.DBCore.IgnoreStr + " like '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " " + p.DBCore.IgnoreStr + " like '" + v + "'"
				} else {
					subSQL += " or " + colName + " " + p.DBCore.IgnoreStr + " like '" + v + "'"
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
func (orm *ORM) sqliteUpsertSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()

	if data == nil {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	colSet := set.New[string]()
	var values []string
	var update []string

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
				colSet.Add(k)
				values = append(values, fmt.Sprintf("%v", v))
				update = append(update, fmt.Sprintf("%s%s%s=EXCLUDED.%s%s%s",
					dbCore.EscStart, k, dbCore.EscEnd,
					dbCore.EscStart, k, dbCore.EscEnd))
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
				colSet.Add(k)
				values = append(values, val)
			}
			update = append(update, fmt.Sprintf("%s%s%s=EXCLUDED.%s%s%s",
				dbCore.EscStart, k, dbCore.EscEnd,
				dbCore.EscStart, k, dbCore.EscEnd))
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
			} else {
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, colName, dbCore.EscEnd))
				colSet.Add(colName)
				values = append(values, val)
			}
			update = append(update, fmt.Sprintf("%s%s%s=EXCLUDED.%s%s%s",
				dbCore.EscStart, colName, dbCore.EscEnd,
				dbCore.EscStart, colName, dbCore.EscEnd))
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

	upsertSQL := "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s) " +
		"ON conflict(%s) DO update set %s"
	if hasPK {
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","),
			util.JoinArr(values, ","), orm.primaryKey, util.JoinArr(update, ","))
	} else if hasUK {
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","),
			util.JoinArr(values, ","),
			connectStrArr(orm.uniqueKeys.ToList(), ",", dbCore.EscStart, dbCore.EscEnd),
			util.JoinArr(update, ","))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	}

	return upsertSQL, nil
}

func (orm *ORM) sqliteUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
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
	updateCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		newCols = append(newCols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
		updateCols = append(updateCols, fmt.Sprintf("%s%s%s=EXCLUDED.%s%s%s",
			dbCore.EscStart, k, dbCore.EscEnd,
			dbCore.EscStart, k, dbCore.EscEnd))
	}

	colSet := set.New[string]()
	colSet.Add(cols...)

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	upsertSQL := "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s " +
		"ON conflict(%s) DO update set %s"
	if hasPK {
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","),
			util.JoinArr(valArr, ","), orm.primaryKey, util.JoinArr(updateCols, ","))
	} else if hasUK {
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","),
			util.JoinArr(valArr, ","),
			connectStrArr(orm.uniqueKeys.ToList(), ",", dbCore.EscStart, dbCore.EscEnd),
			util.JoinArr(updateCols, ","))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	}

	return upsertSQL, nil
}
