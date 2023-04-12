package orm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

const oracleTimeFormat = "yyyy-mm-dd hh24:mi:ss"
const oracleDateFormat = "yyyy-mm-dd"

func oracleDateTime(s string, isDate bool) string {
	var strBuf strings.Builder
	strBuf.Grow(50)
	strBuf.WriteString("TO_DATE('")
	strBuf.WriteString(s)
	strBuf.WriteString("','")
	if isDate {
		strBuf.WriteString(oracleDateFormat)
	} else {
		strBuf.WriteString(oracleTimeFormat)
	}
	strBuf.WriteString("')")
	return strBuf.String()
}

// queryModel
func (p *queryModel) queryOracle() string {
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
		sql.WriteString(" ")
		sql.WriteString(p.MainAlias)
	}

	join := p.joinSQL()
	if join != "" {
		sql.WriteString(join)
	}

	limitSQL := ""
	if len(p.Limit) == 1 {
		limitSQL += "rownum<=" + util.UintToStr(p.Limit[0])
	}

	where := p.andSQL(p.Where)
	if where != "" {
		if limitSQL != "" {
			limitSQL = " and " + limitSQL
		}
		sql.WriteString(" where ")
		sql.WriteString(where)
		sql.WriteString(limitSQL)
	} else if limitSQL != "" {
		sql.WriteString(" where ")
		sql.WriteString(limitSQL)
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

	if len(p.Limit) == 2 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(util.UintToStr(p.Limit[0]))
		sql.WriteString(" ROWS FETCH NEXT ")
		sql.WriteString(util.UintToStr(p.Limit[1]))
		sql.WriteString(" ROWS ONLY")
	}

	if p.SelectForUpdate {
		sql.WriteString(" for update")
	}

	return sql.String()
}

func (p *queryModel) oracleIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	var subSQL strings.Builder
	subSQL.Grow(20)

	subSQL.WriteString("LOWER(")
	subSQL.WriteString(colName)
	subSQL.WriteByte(')')
	switch colOperator {
	case "eq":
		subSQL.WriteString("=LOWER(")
	case "lt":
		subSQL.WriteString("<LOWER(")
	case "lte":
		subSQL.WriteString("<=LOWER(")
	case "gt":
		subSQL.WriteString(">LOWER(")
	case "gte":
		subSQL.WriteString(">=LOWER(")
	case "ne":
		subSQL.WriteString("<>LOWER(")
	case "in":
		val = connectStrArr(rawStrArr, ",", "LOWER('", "')")
		subSQL.WriteString(" in (")
	case "nin":
		val = connectStrArr(rawStrArr, ",", "LOWER('", "')")
		subSQL.WriteString(" not in (")
	default:
		return p.oracleIgnoreLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	subSQL.WriteString(val)
	subSQL.WriteByte(')')
	return subSQL.String()
}

func (p *queryModel) oracleIgnoreLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%')")
		}
	case "endswith":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("')")
		}
	case "contains":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%')")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString("LOWER(")
				subSQL.WriteString(colName)
				subSQL.WriteString(") like LOWER('%")
				subSQL.WriteString(v)
				subSQL.WriteString("%')")
			}
			subSQL.WriteByte(')')
		}
	case "customlike":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("')")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString("LOWER(")
				subSQL.WriteString(colName)
				subSQL.WriteString(") like LOWER('")
				subSQL.WriteString(v)
				subSQL.WriteString("')")
			}
			subSQL.WriteByte(')')
		}
	default:
		return p.oracleIgnoreOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) oracleIgnoreOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%')")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString("LOWER(")
				subSQL.WriteString(colName)
				subSQL.WriteString(") like LOWER('")
				subSQL.WriteString(v)
				subSQL.WriteString("%')")
			}
			subSQL.WriteByte(')')
		}
	case "orendswith":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("')")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString("LOWER(")
				subSQL.WriteString(colName)
				subSQL.WriteString(") like LOWER('%")
				subSQL.WriteString(v)
				subSQL.WriteString("')")
			}
			subSQL.WriteByte(')')
		}
	case "orcontains":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%')")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString("LOWER(")
				subSQL.WriteString(colName)
				subSQL.WriteString(") like LOWER('%")
				subSQL.WriteString(v)
				subSQL.WriteString("%')")
			}
			subSQL.WriteByte(')')
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL.WriteString("LOWER(")
			subSQL.WriteString(colName)
			subSQL.WriteString(") like LOWER('")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("')")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString("LOWER(")
				subSQL.WriteString(colName)
				subSQL.WriteString(") like LOWER('")
				subSQL.WriteString(v)
				subSQL.WriteString("')")
			}
			subSQL.WriteByte(')')
		}
	default:
		panic("no definition")
	}
	return subSQL.String()
}

// orm
func (orm *ORM) oracleInsertManySQL(dataList []interface{}, cols []string) (string, error) {
	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	dbCore := orm.ref.getDBConf()
	var insertSQL strings.Builder
	insertSQL.Grow(len(dataList)*len(cols)*25 + 200)

	insertSQL.WriteString("insert all")

	var tableBuff strings.Builder
	tableBuff.WriteString(dbCore.EscStart)
	tableBuff.WriteString(orm.tableName)
	tableBuff.WriteString(dbCore.EscEnd)
	tableBuff.WriteByte('(')
	for i := range cols {
		err := globalVerifyObj.VerifyFieldName(cols[i])
		if err != nil {
			return "", err
		}
		if i > 0 {
			tableBuff.WriteByte(',')
		}
		tableBuff.WriteString(dbCore.EscStart)
		tableBuff.WriteString(cols[i])
		tableBuff.WriteString(dbCore.EscEnd)
	}
	tableBuff.WriteByte(')')
	tableAndField := tableBuff.String()

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
		valArr = append(valArr, subVal.String())
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("insert data is empty")
	}

	for i := 0; i < len(valArr); i++ {
		insertSQL.WriteString(" into ")
		insertSQL.WriteString(tableAndField)
		insertSQL.WriteString(" VALUES(")
		insertSQL.WriteString(valArr[i])
		insertSQL.WriteByte(')')
	}

	insertSQL.WriteString(" select * from \"DUAL\"")
	return insertSQL.String(), nil
}

func (orm *ORM) oracleUpsertSQL(data interface{}) (string, error) {
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
		upsertSQL.WriteString(" \"T\" USING (SELECT ")
		for i := range formatCols {
			if i > 0 {
				upsertSQL.WriteByte(',')
			}
			upsertSQL.WriteString(rawVal[i])
			upsertSQL.WriteString(" as ")
			upsertSQL.WriteString(formatCols[i])
		}
		upsertSQL.WriteString(" from \"DUAL\") \"S\" ON (")

		if hasPK {
			upsertSQL.WriteString("\"T\".")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
			upsertSQL.WriteString("=\"S\".")
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

				onStrBuff.WriteString("\"T\".")
				onStrBuff.WriteString(dbCore.EscStart)
				onStrBuff.WriteString(item)
				onStrBuff.WriteString(dbCore.EscEnd)
				onStrBuff.WriteString("=\"S\".")
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

				upsertSQL.WriteString("\"T\".")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=\"S\".")
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

				upsertSQL.WriteString("\"T\".")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=\"S\".")
				upsertSQL.WriteString(formatCols[i])
			}
		}
		upsertSQL.WriteString(" WHEN NOT MATCHED THEN INSERT(")
		upsertSQL.WriteString(util.JoinArr(formatCols, ","))
		upsertSQL.WriteString(" VALUES(")
		upsertSQL.WriteString(connectStrArr(formatCols, ",", "\"S\".", ""))
		upsertSQL.WriteByte(')')
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

func (orm *ORM) oracleUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
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

	unionStr := " UNION "
	if orm.oracleMergeUnionAll {
		unionStr = " UNION ALL "
	}
	if hasPK || hasUK {
		var upsertSQL strings.Builder
		upsertSQL.Grow(len(formatCols)*10 + 100)

		upsertSQL.WriteString("MERGE INTO ")
		upsertSQL.WriteString(dbCore.EscStart)
		upsertSQL.WriteString(orm.tableName)
		upsertSQL.WriteString(dbCore.EscEnd)
		upsertSQL.WriteString(" \"T\" USING (")
		for index := range rawVal {
			if index > 0 {
				upsertSQL.WriteString(unionStr)
			}
			upsertSQL.WriteString("SELECT ")
			for i := range formatCols {
				if i > 0 {
					upsertSQL.WriteByte(',')
				}
				upsertSQL.WriteString(rawVal[index][i])
				upsertSQL.WriteString(" as ")
				upsertSQL.WriteString(formatCols[i])
			}
			upsertSQL.WriteString(" from \"DUAL\"")
		}

		upsertSQL.WriteString(") \"S\" ON (")

		if hasPK {
			upsertSQL.WriteString("\"T\".")
			upsertSQL.WriteString(dbCore.EscStart)
			upsertSQL.WriteString(orm.primaryKey)
			upsertSQL.WriteString(dbCore.EscEnd)
			upsertSQL.WriteString("=\"S\".")
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

				onStrBuff.WriteString("\"T\".")
				onStrBuff.WriteString(dbCore.EscStart)
				onStrBuff.WriteString(item)
				onStrBuff.WriteString(dbCore.EscEnd)
				onStrBuff.WriteString("=\"S\".")
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

				upsertSQL.WriteString("\"T\".")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=\"S\".")
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

				upsertSQL.WriteString("\"T\".")
				upsertSQL.WriteString(formatCols[i])
				upsertSQL.WriteString("=\"S\".")
				upsertSQL.WriteString(formatCols[i])
			}
		}
		upsertSQL.WriteString(" WHEN NOT MATCHED THEN INSERT(")
		upsertSQL.WriteString(util.JoinArr(formatCols, ","))
		upsertSQL.WriteString(" VALUES(")
		upsertSQL.WriteString(connectStrArr(formatCols, ",", "\"S\".", ""))
		upsertSQL.WriteByte(')')
		return upsertSQL.String(), nil
	}

	var insertSQL strings.Builder
	insertSQL.Grow(len(rawVal)*len(cols)*25 + 200)

	insertSQL.WriteString("insert all")

	var tableBuff strings.Builder
	tableBuff.Grow(len(rawVal)*len(cols)*5 + 100)
	tableBuff.WriteString(dbCore.EscStart)
	tableBuff.WriteString(orm.tableName)
	tableBuff.WriteString(dbCore.EscEnd)
	tableBuff.WriteByte('(')
	for i := range formatCols {
		if i > 0 {
			tableBuff.WriteByte(',')
		}
		tableBuff.WriteString(formatCols[i])
	}
	tableBuff.WriteByte(')')
	tableAndField := tableBuff.String()

	for i := 0; i < len(rawVal); i++ {
		insertSQL.WriteString(" into ")
		insertSQL.WriteString(tableAndField)
		insertSQL.WriteString(" VALUES(")
		for j := 0; j < len(rawVal[i]); j++ {
			if j > 0 {
				insertSQL.WriteByte(',')
			}
			insertSQL.WriteString(rawVal[i][j])
		}
		insertSQL.WriteByte(')')
	}

	insertSQL.WriteString(" select * from \"DUAL\"")
	return insertSQL.String(), nil
}
