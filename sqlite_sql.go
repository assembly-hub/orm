package orm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

func (p *queryModel) querySQLite() string {
	var sql strings.Builder
	sql.Grow(100)
	sql.WriteString("select ")

	sel := p.selectSQL()
	if p.MainTable == "" {
		panic("MainTable is nil")
	}

	sql.WriteString(sel)
	sql.WriteString(" from ")
	sql.WriteString(p.MainTable)
	if p.MainAlias != "" {
		sql.WriteString(" as ")
		sql.WriteString(p.MainAlias)
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

	order := p.orderSQL()
	if order != "" {
		sql.WriteString(" order by ")
		sql.WriteString(order)
	}

	if len(p.Limit) == 1 {
		sql.WriteString(" limit ")
		sql.WriteString(util.UintToStr(p.Limit[0]))
	} else if len(p.Limit) == 2 {
		sql.WriteString(" limit ")
		sql.WriteString(util.UintToStr(p.Limit[1]))
		sql.WriteString(" offset ")
		sql.WriteString(util.UintToStr(p.Limit[0]))
	}

	//if p.SelectForUpdate {
	//	sql += " for update"
	//}

	return sql.String()
}

func (p *queryModel) sqliteIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	subSQL.WriteString(colName)
	subSQL.WriteByte(' ')
	subSQL.WriteString(p.DBCore.IgnoreStr)
	switch colOperator {
	case "eq":
		subSQL.WriteString(" =")
	case "lt":
		subSQL.WriteString(" <")
	case "lte":
		subSQL.WriteString(" <=")
	case "gt":
		subSQL.WriteString(" >")
	case "gte":
		subSQL.WriteString(" >=")
	case "ne":
		subSQL.WriteString(" <>")
	case "in":
		subSQL.WriteString(" in ")
	case "nin":
		subSQL.WriteString(" not in ")
	default:
		return p.sqliteIgnoreLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	subSQL.WriteString(val)
	return subSQL.String()
}

func (p *queryModel) sqliteIgnoreLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		}
	case "endswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		}
	case "contains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
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
				subSQL.WriteByte(' ')
				subSQL.WriteString(p.DBCore.IgnoreStr)
				subSQL.WriteString(" like '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "customlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteByte(' ')
				subSQL.WriteString(p.DBCore.IgnoreStr)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		return p.sqliteIgnoreOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) sqliteIgnoreOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteByte(' ')
				subSQL.WriteString(p.DBCore.IgnoreStr)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orendswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteByte(' ')
				subSQL.WriteString(p.DBCore.IgnoreStr)
				subSQL.WriteString(" like '%")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	case "orcontains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteByte(' ')
				subSQL.WriteString(p.DBCore.IgnoreStr)
				subSQL.WriteString(" like '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.IgnoreStr)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteByte(' ')
				subSQL.WriteString(p.DBCore.IgnoreStr)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		panic("no definition")
	}
	return subSQL.String()
}

// orm
func (orm *ORM) sqliteUpsertSQL(data interface{}) (string, error) {
	dbCore := orm.ref.getDBConf()
	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	var upsertSQL strings.Builder
	upsertSQL.Grow(100)
	upsertSQL.WriteString("insert into ")
	upsertSQL.WriteString(dbCore.EscStart)
	upsertSQL.WriteString(orm.tableName)
	upsertSQL.WriteString(dbCore.EscEnd)
	upsertSQL.WriteByte('(')

	if data == nil {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of upsert data is map[string]interface{} or *struct or struct"
	var formatCols []string
	var values []string
	colSet := set.New[string]()

	switch data := data.(type) {
	case map[string]interface{}:
		formatCols = make([]string, 0, len(data))
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

				colSet.Add(k)
				formatCols = append(formatCols, formatKey.String())
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

				colSet.Add(k)
				formatCols = append(formatCols, formatKey.String())
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

				colSet.Add(colName)
				formatCols = append(formatCols, formatKey.String())
				values = append(values, val)
			}
		}
	}
	if len(formatCols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	upsertSQL.WriteString(util.JoinArr(formatCols, ","))
	upsertSQL.WriteString(") values(")
	upsertSQL.WriteString(util.JoinArr(values, ","))
	upsertSQL.WriteByte(')')
	if hasPK || hasUK {
		upsertSQL.WriteString(" ON conflict(")

		excludeSet := set.New[string]()
		if hasPK {
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
			excludeSet.Add(orm.primaryKey)
		} else {
			hasData := false
			orm.uniqueKeys.Range(func(item string) bool {
				if hasData {
					upsertSQL.WriteByte(',')
				}
				hasData = true

				upsertSQL.WriteString(dbCore.EscStart)
				upsertSQL.WriteString(item)
				upsertSQL.WriteString(dbCore.EscEnd)
				return true
			})
			excludeSet = orm.uniqueKeys
		}

		upsertSQL.WriteString(") DO update set ")

		hasData := false
		for k := range colSet {
			if excludeSet.Has(k) {
				continue
			}

			if hasData {
				upsertSQL.WriteByte(',')
			}
			hasData = true

			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(k)
			upsertSQL.WriteString(dbCore.EscEnd)
			upsertSQL.WriteString("=EXCLUDED.")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(k)
			upsertSQL.WriteString(dbCore.EscEnd)
		}

		if !hasData {
			return "", fmt.Errorf("conflict excluded field is empty")
		}
	}

	return upsertSQL.String(), nil
}

func (orm *ORM) sqliteUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	dbCore := orm.ref.getDBConf()

	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	var upsertSQL strings.Builder
	upsertSQL.Grow(len(dataList)*len(cols)*5 + 100)

	upsertSQL.WriteString("insert into ")
	upsertSQL.WriteString(dbCore.EscStart)
	upsertSQL.WriteString(orm.tableName)
	upsertSQL.WriteString(dbCore.EscEnd)

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"

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

	colSet := set.New[string]()
	formatCols := make([]string, 0, len(cols))
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

		formatCols = append(formatCols, buf.String())
		colSet.Add(k)
	}

	upsertSQL.WriteByte('(')
	upsertSQL.WriteString(util.JoinArr(formatCols, ","))
	upsertSQL.WriteString(") values")
	upsertSQL.WriteString(util.JoinArr(valArr, ","))

	hasPK := colSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(colSet)
	}

	if hasPK || hasUK {
		upsertSQL.WriteString(" ON conflict(")

		excludeSet := set.New[string]()
		if hasPK {
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
			excludeSet.Add(orm.primaryKey)
		} else {
			hasData := false
			orm.uniqueKeys.Range(func(item string) bool {
				if hasData {
					upsertSQL.WriteByte(',')
				}
				hasData = true

				upsertSQL.WriteString(dbCore.EscStart)
				upsertSQL.WriteString(item)
				upsertSQL.WriteString(dbCore.EscEnd)
				return true
			})
			excludeSet = orm.uniqueKeys
		}

		upsertSQL.WriteString(") DO update set ")

		hasData := false
		for k := range colSet {
			if excludeSet.Has(k) {
				continue
			}

			if hasData {
				upsertSQL.WriteByte(',')
			}
			hasData = true

			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(k)
			upsertSQL.WriteString(dbCore.EscEnd)
			upsertSQL.WriteString("=EXCLUDED.")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(k)
			upsertSQL.WriteString(dbCore.EscEnd)
		}

		if !hasData {
			return "", fmt.Errorf("conflict excluded field is empty")
		}
	}

	return upsertSQL.String(), nil
}
