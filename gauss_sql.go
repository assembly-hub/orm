package orm

import (
	"fmt"
	"reflect"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

// orm
func (orm *ORM) gaussUpsertSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()

	if data == nil {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
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
				cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
				colSet.Add(k)
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
			}

			cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, k, dbCore.EscEnd))
			colSet.Add(k)
			values = append(values, val)
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

			cols = append(cols, fmt.Sprintf("%s%s%s", dbCore.EscStart, colName, dbCore.EscEnd))
			colSet.Add(colName)
			values = append(values, val)
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

	upsertSQL := "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s) " +
		"on duplicate key update %s"
	if hasPK {
		colSet.Del(orm.primaryKey)
		if colSet.Empty() {
			return "", fmt.Errorf("no data")
		}
		colSet.Range(func(item string) bool {
			update = append(update, fmt.Sprintf("%s%s%s=values(%s%s%s)",
				dbCore.EscStart, item, dbCore.EscEnd, dbCore.EscStart, item, dbCore.EscEnd))
			return hasUK
		})
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","),
			util.JoinArr(values, ","), util.JoinArr(update, ","))
	} else if hasUK {
		colSet.Del(orm.uniqueKeys.ToList()...)
		if colSet.Empty() {
			return "", fmt.Errorf("no data")
		}
		colSet.Range(func(item string) bool {
			update = append(update, fmt.Sprintf("%s%s%s=values(%s%s%s)",
				dbCore.EscStart, item, dbCore.EscEnd, dbCore.EscStart, item, dbCore.EscEnd))
			return hasUK
		})
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","),
			util.JoinArr(values, ","), util.JoinArr(update, ","))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	}

	return upsertSQL, nil
}

func (orm *ORM) gaussUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
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
	}

	colSet := set.New[string]()
	colSet.Add(cols...)

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	upsertSQL := "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values%s " +
		"on duplicate key update %s"
	if hasPK {
		colSet.Del(orm.primaryKey)
		if colSet.Empty() {
			return "", fmt.Errorf("no data")
		}
		colSet.Range(func(item string) bool {
			updateCols = append(updateCols, fmt.Sprintf("%s%s%s=values(%s%s%s)",
				dbCore.EscStart, item, dbCore.EscEnd, dbCore.EscStart, item, dbCore.EscEnd))
			return hasUK
		})
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","),
			util.JoinArr(valArr, ","), util.JoinArr(updateCols, ","))
	} else if hasUK {
		colSet.Del(orm.uniqueKeys.ToList()...)
		if colSet.Empty() {
			return "", fmt.Errorf("no data")
		}
		colSet.Range(func(item string) bool {
			updateCols = append(updateCols, fmt.Sprintf("%s%s%s=values(%s%s%s)",
				dbCore.EscStart, item, dbCore.EscEnd, dbCore.EscStart, item, dbCore.EscEnd))
			return hasUK
		})
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","),
			util.JoinArr(valArr, ","), util.JoinArr(updateCols, ","))
	} else {
		upsertSQL = "insert into " + dbCore.EscStart + "%s" + dbCore.EscEnd + "(%s) values(%s)"
		upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	}

	return upsertSQL, nil
}
