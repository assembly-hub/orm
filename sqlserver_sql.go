package orm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

func (p *queryModel) querySQLServer() string {
	var sql strings.Builder
	sql.Grow(100)
	sql.WriteString("select ")

	if len(p.Limit) == 1 {
		sql.WriteString(" top(")
		sql.WriteString(util.UintToStr(p.Limit[0]))
		sql.WriteString(") ")
	}
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
	orderSQL := ""
	if order != "" {
		sql.WriteString(" order by ")
		sql.WriteString(order)
	} else {
		orderSQL = " order by " + p.DBCore.EscStart + p.PrivateKey + p.DBCore.EscEnd
	}

	if len(p.Limit) == 2 {
		sql.WriteString(orderSQL)
		sql.WriteString(" offset ")
		sql.WriteString(util.UintToStr(p.Limit[0]))
		sql.WriteString(" rows fetch next ")
		sql.WriteString(util.UintToStr(p.Limit[1]))
		sql.WriteString(" rows only")
	}

	// SelectMethod=cursor
	if p.SelectForUpdate {
		sql.WriteString(" for update")
	}

	return sql.String()
}

func (p *queryModel) sqlserverBinFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	subSQL.WriteString(colName)
	subSQL.WriteByte(' ')
	subSQL.WriteString(p.DBCore.BinStr)

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
		return p.sqlserverBinLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	subSQL.WriteString(val)
	return subSQL.String()
}

func (p *queryModel) sqlserverBinLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" like '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		}
	case "endswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" like '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		}
	case "contains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.BinStr)
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
				subSQL.WriteString(p.DBCore.BinStr)
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
			subSQL.WriteString(p.DBCore.BinStr)
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
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" like '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		return p.sqlserverBinOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) sqlserverBinOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteByte(' ')
			subSQL.WriteString(p.DBCore.BinStr)
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
				subSQL.WriteString(p.DBCore.BinStr)
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
			subSQL.WriteString(p.DBCore.BinStr)
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
				subSQL.WriteString(p.DBCore.BinStr)
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
			subSQL.WriteString(p.DBCore.BinStr)
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
				subSQL.WriteString(p.DBCore.BinStr)
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
			subSQL.WriteString(p.DBCore.BinStr)
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
				subSQL.WriteString(p.DBCore.BinStr)
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
func (orm *ORM) sqlserverUpsertSQL(data interface{}) (string, error) {
	if data == nil {
		return "", fmt.Errorf("upsert data is nil")
	}

	dbCore := orm.ref.getDBConf()
	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	typeErrStr := "type of upsert data is map[string]interface{} or *struct or struct"
	var formatCols []string
	rawColSet := set.New[string]()
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
				var formatKey strings.Builder
				formatKey.Grow(escLen + len(k))
				formatKey.WriteString(dbCore.EscStart)
				formatKey.WriteString(k)
				formatKey.WriteString(dbCore.EscEnd)

				formatCols = append(formatCols, formatKey.String())
				rawColSet.Add(k)

				val := util.InterfaceToString(v)
				rawVal = append(rawVal, val)
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
			var formatKey strings.Builder
			formatKey.Grow(escLen + len(k))
			formatKey.WriteString(dbCore.EscStart)
			formatKey.WriteString(k)
			formatKey.WriteString(dbCore.EscEnd)

			formatCols = append(formatCols, formatKey.String())
			rawColSet.Add(k)
			rawVal = append(rawVal, val)
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

			var formatKey strings.Builder
			formatKey.Grow(escLen + len(colName))
			formatKey.WriteString(dbCore.EscStart)
			formatKey.WriteString(colName)
			formatKey.WriteString(dbCore.EscEnd)

			formatCols = append(formatCols, formatKey.String())
			rawColSet.Add(colName)
			rawVal = append(rawVal, val)
		}
	}
	if len(formatCols) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	hasPK := rawColSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(rawColSet)
	}

	if hasPK || hasUK {
		var upsertSQL strings.Builder
		upsertSQL.Grow(len(formatCols)*10 + 100)

		upsertSQL.WriteString("MERGE INTO ")
		upsertSQL.WriteString(dbCore.EscStart)
		upsertSQL.WriteString(orm.tableName)
		upsertSQL.WriteString(dbCore.EscEnd)
		upsertSQL.WriteString(" as [T] USING (values(")
		for i := range rawVal {
			if i > 0 {
				upsertSQL.WriteByte(',')
			}
			upsertSQL.WriteString(rawVal[i])
		}
		upsertSQL.WriteString(")) as [S] ON (")

		if hasPK {
			upsertSQL.WriteString("[T].")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
			upsertSQL.WriteString("=[S].")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
		} else {
			var onStrBuff strings.Builder
			onStrBuff.Grow(orm.uniqueKeys.Size() * (escLen + 6 + 10))
			orm.uniqueKeys.Range(func(item string) bool {
				if onStrBuff.Len() > 0 {
					onStrBuff.WriteString(" and ")
				}

				onStrBuff.WriteString("[T].")
				onStrBuff.WriteString(dbCore.EscStart)
				onStrBuff.WriteString(item)
				onStrBuff.WriteString(dbCore.EscEnd)
				onStrBuff.WriteString("=[S].")
				onStrBuff.WriteString(dbCore.EscStart)
				onStrBuff.WriteString(item)
				onStrBuff.WriteString(dbCore.EscEnd)
				return true
			})
			upsertSQL.WriteString(onStrBuff.String())
		}
		upsertSQL.WriteString(") WHEN MATCHED THEN UPDATE SET ")
		if hasPK {
			has := false
			formatPK := dbCore.EscStart + orm.primaryKey + dbCore.EscEnd
			for i := range formatCols {
				if formatPK == formatCols[i] {
					continue
				}

				if has {
					upsertSQL.WriteByte(',')
				}
				has = true

				upsertSQL.WriteString("[T].")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=[S].")
				upsertSQL.WriteString(formatCols[i])
			}
		} else {
			has := false
			for i := range formatCols {
				k := formatCols[i]
				if orm.uniqueKeys.Has(k[len(dbCore.EscStart) : len(k)-len(dbCore.EscEnd)]) {
					continue
				}

				if has {
					upsertSQL.WriteByte(',')
				}
				has = true

				upsertSQL.WriteString("[T].")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=[S].")
				upsertSQL.WriteString(formatCols[i])
			}
		}
		upsertSQL.WriteString(" WHEN NOT MATCHED THEN INSERT(")
		upsertSQL.WriteString(util.JoinArr(formatCols, ","))
		upsertSQL.WriteString(" VALUES(")
		upsertSQL.WriteString(connectStrArr(formatCols, ",", "[S].", ""))
		upsertSQL.WriteString(");")
		return upsertSQL.String(), nil
	}

	var upsertSQL strings.Builder
	upsertSQL.WriteString("insert into ")
	upsertSQL.WriteString(dbCore.EscStart)
	upsertSQL.WriteString(orm.tableName)
	upsertSQL.WriteString(dbCore.EscEnd)
	upsertSQL.WriteByte('(')
	upsertSQL.WriteString(util.JoinArr(formatCols, ","))
	upsertSQL.WriteString(") VALUES(")
	upsertSQL.WriteString(util.JoinArr(rawVal, ","))
	upsertSQL.WriteByte(')')
	return upsertSQL.String(), nil
}

func (orm *ORM) sqlserverUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
	if len(dataList) <= 0 {
		return "", fmt.Errorf("upsert data is nil")
	}

	dbCore := orm.ref.getDBConf()
	escLen := len(dbCore.EscStart) + len(dbCore.EscEnd)

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"

	formatCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		var formatKey strings.Builder
		formatKey.Grow(escLen + len(k))
		formatKey.WriteString(dbCore.EscStart)
		formatKey.WriteString(k)
		formatKey.WriteString(dbCore.EscEnd)

		formatCols = append(formatCols, formatKey.String())
	}

	rawColSet := set.New[string]()
	rawColSet.Add(cols...)

	rawVal := make([][]string, 0, len(dataList))
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

		subVal := make([]string, 0, len(cols))
		for _, colName := range cols {
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := orm.formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal = append(subVal, "null")
					continue
				}

				subVal = append(subVal, val)
			} else if v, ok = valMap["#"+colName]; ok {
				subVal = append(subVal, util.InterfaceToString(v))
			} else {
				subVal = append(subVal, "null")
			}
		}
		rawVal = append(rawVal, subVal)
	}

	if len(rawVal) <= 0 {
		return "", fmt.Errorf("upsert data is empty")
	}

	hasPK := rawColSet.Has(orm.primaryKey)
	hasUK := false
	if !hasPK {
		hasUK = orm.checkUK(rawColSet)
	}

	if hasPK || hasUK {
		var upsertSQL strings.Builder
		upsertSQL.Grow(len(formatCols)*10 + 100)

		upsertSQL.WriteString("MERGE INTO ")
		upsertSQL.WriteString(dbCore.EscStart)
		upsertSQL.WriteString(orm.tableName)
		upsertSQL.WriteString(dbCore.EscEnd)
		upsertSQL.WriteString(" as [T] USING (values")
		for index := range rawVal {
			if index > 0 {
				upsertSQL.WriteByte(',')
			}
			upsertSQL.WriteString("(")
			for i := range formatCols {
				if i > 0 {
					upsertSQL.WriteByte(',')
				}
				upsertSQL.WriteString(rawVal[index][i])
			}
			upsertSQL.WriteString(")")
		}

		upsertSQL.WriteString(") as [S] ON (")

		if hasPK {
			upsertSQL.WriteString("[T].")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
			upsertSQL.WriteString("=[S].")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
		} else {
			var onStrBuff strings.Builder
			onStrBuff.Grow(orm.uniqueKeys.Size() * (escLen + 6 + 10))
			orm.uniqueKeys.Range(func(item string) bool {
				if onStrBuff.Len() > 0 {
					onStrBuff.WriteString(" and ")
				}

				onStrBuff.WriteString("[T].")
				onStrBuff.WriteString(dbCore.EscStart)
				onStrBuff.WriteString(item)
				onStrBuff.WriteString(dbCore.EscEnd)
				onStrBuff.WriteString("=[S].")
				onStrBuff.WriteString(dbCore.EscStart)
				onStrBuff.WriteString(item)
				onStrBuff.WriteString(dbCore.EscEnd)
				return true
			})
			upsertSQL.WriteString(onStrBuff.String())
		}
		upsertSQL.WriteString(") WHEN MATCHED THEN UPDATE SET ")
		if hasPK {
			has := false
			formatPK := dbCore.EscStart + orm.primaryKey + dbCore.EscEnd
			for i := range formatCols {
				if formatPK == formatCols[i] {
					continue
				}

				if has {
					upsertSQL.WriteByte(',')
				}
				has = true

				upsertSQL.WriteString("[T].")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=[S].")
				upsertSQL.WriteString(formatCols[i])
			}
		} else {
			has := false
			for i := range formatCols {
				k := formatCols[i]
				if orm.uniqueKeys.Has(k[len(dbCore.EscStart) : len(k)-len(dbCore.EscEnd)]) {
					continue
				}

				if has {
					upsertSQL.WriteByte(',')
				}
				has = true

				upsertSQL.WriteString("[T].")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=[S].")
				upsertSQL.WriteString(formatCols[i])
			}
		}
		upsertSQL.WriteString(" WHEN NOT MATCHED THEN INSERT(")
		upsertSQL.WriteString(util.JoinArr(formatCols, ","))
		upsertSQL.WriteString(" VALUES(")
		upsertSQL.WriteString(connectStrArr(formatCols, ",", "[S].", ""))
		upsertSQL.WriteString(");")
		return upsertSQL.String(), nil
	}

	var insertSQL strings.Builder
	insertSQL.Grow(len(rawVal)*len(cols)*5 + 200)

	insertSQL.WriteString("insert into ")
	insertSQL.WriteString(dbCore.EscStart)
	insertSQL.WriteString(orm.tableName)
	insertSQL.WriteString(dbCore.EscEnd)
	insertSQL.WriteByte('(')
	for i := range formatCols {
		if i > 0 {
			insertSQL.WriteByte(',')
		}
		insertSQL.WriteString(formatCols[i])
	}
	insertSQL.WriteString(") values")

	for i := 0; i < len(rawVal); i++ {
		if i > 0 {
			insertSQL.WriteByte(',')
		}
		insertSQL.WriteByte('(')
		for j := 0; j < len(rawVal[i]); j++ {
			if j > 0 {
				insertSQL.WriteByte(',')
			}
			insertSQL.WriteString(rawVal[i][j])
		}
		insertSQL.WriteByte(')')
	}
	return insertSQL.String(), nil
}
