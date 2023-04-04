package orm

import (
	"fmt"
	"reflect"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

func (p *queryModel) postgresIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	newCol := "LOWER(" + colName + ")"
	newVal := "LOWER(" + val + ")"
	switch colOperator {
	case "eq":
		subSQL = newCol + "=" + newVal
	case "lt":
		subSQL = newCol + "<" + newVal
	case "lte":
		subSQL = newCol + "<=" + newVal
	case "gt":
		subSQL = newCol + ">" + newVal
	case "gte":
		subSQL = newCol + ">=" + newVal
	case "ne":
		subSQL = newCol + "<>" + newVal
	case "in":
		newVal = "(" + connectStrArr(rawStrArr, ",", "LOWER('", "')") + ")"
		subSQL = newCol + " in " + newVal
	case "nin":
		newVal = "(" + connectStrArr(rawStrArr, ",", "LOWER('", "')") + ")"
		subSQL = newCol + " not in " + newVal
	default:
		subSQL = p.postgresIgnoreLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) postgresIgnoreLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL = colName + " ilike '" + rawVal + "%'"
		}
	case "endswith":
		if rawVal != "" {
			subSQL = colName + " ilike '%" + rawVal + "'"
		}
	case "contains":
		if rawVal != "" {
			subSQL = colName + " ilike '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " ilike '%" + v + "%'"
				} else {
					subSQL += " and " + colName + " ilike '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "customlike":
		if rawVal != "" {
			subSQL = colName + " ilike '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " ilike '" + v + "'"
				} else {
					subSQL += " and " + colName + " ilike '" + v + "'"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.postgresIgnoreOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL
}

func (p *queryModel) postgresIgnoreOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL = colName + " ilike '" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " ilike '" + v + "%'"
				} else {
					subSQL += " or " + colName + " ilike '" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orendswith":
		if rawVal != "" {
			subSQL = colName + " ilike '%" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " ilike '%" + v + "'"
				} else {
					subSQL += " or " + colName + " ilike '%" + v + "'"
				}
			}
			subSQL += ")"
		}
	case "orcontains":
		if rawVal != "" {
			subSQL = colName + " ilike '%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " ilike '%" + v + "%'"
				} else {
					subSQL += " or " + colName + " ilike '%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL = colName + " ilike '" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " ilike '" + v + "'"
				} else {
					subSQL += " or " + colName + " ilike '" + v + "'"
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
func (orm *ORM) postgresUpsertSQL(data interface{}) (string, error) {
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

func (orm *ORM) postgresUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
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
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	}

	return upsertSQL, nil
}
