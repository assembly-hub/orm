package orm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/assembly-hub/basics/util"
)

func (p *queryModel) queryMySQL() string {
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

	if p.SelectForUpdate {
		sql.WriteString(" for update")
	}

	return sql.String()
}

func (p *queryModel) mysqlBinFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "eq":
		subSQL.WriteString(colName)
		subSQL.WriteByte('=')
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteString(val)
	case "lt":
		subSQL.WriteString(colName)
		subSQL.WriteByte('<')
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteString(val)
	case "lte":
		subSQL.WriteString(colName)
		subSQL.WriteString("<=")
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteString(val)
	case "gt":
		subSQL.WriteString(colName)
		subSQL.WriteByte('>')
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteString(val)
	case "gte":
		subSQL.WriteString(colName)
		subSQL.WriteString(">=")
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteString(val)
	case "ne":
		subSQL.WriteString(colName)
		subSQL.WriteString("<>")
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteString(val)
	case "in":
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteByte(' ')
		subSQL.WriteString(colName)
		subSQL.WriteString(" in ")
		subSQL.WriteString(val)
	case "nin":
		subSQL.WriteString(p.DBCore.BinStr)
		subSQL.WriteByte(' ')
		subSQL.WriteString(colName)
		subSQL.WriteString(" not in ")
		subSQL.WriteString(val)
	default:
		return p.mysqlBinLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}

	return subSQL.String()
}

func (p *queryModel) mysqlBinLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		}
	case "endswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		}
	case "contains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteString("(")
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" like ")
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "customlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteString("(")
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" like ")
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		return p.mysqlBinOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) mysqlBinOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '")
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
				subSQL.WriteString(" like ")
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" '")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orendswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '%")
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
				subSQL.WriteString(" like ")
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" '%")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	case "orcontains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '%")
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
				subSQL.WriteString(" like ")
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" like ")
			subSQL.WriteString(p.DBCore.BinStr)
			subSQL.WriteString(" '")
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
				subSQL.WriteString(" like ")
				subSQL.WriteString(p.DBCore.BinStr)
				subSQL.WriteString(" '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		panic(fmt.Sprintf("col[%s] operation[%s] no definition", colName, colOperator))
	}
	return subSQL.String()
}

// orm
func (orm *ORM) mysqlUpsertSQL(data interface{}) (string, error) {
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

				formatCols = append(formatCols, formatKey.String())
				values = append(values, util.Any2String(v))
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

				formatCols = append(formatCols, formatKey.String())
				values = append(values, val)
			}
		}
	}
	if len(formatCols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	upsertSQL.WriteString(util.JoinArr(formatCols, ","))
	upsertSQL.WriteString(") values(")
	upsertSQL.WriteString(util.JoinArr(values, ","))
	upsertSQL.WriteString(") on duplicate key update ")

	for i := 0; i < len(formatCols); i++ {
		if i > 0 {
			upsertSQL.WriteByte(',')
		}
		upsertSQL.WriteString(formatCols[i])
		upsertSQL.WriteString("=values(")
		upsertSQL.WriteString(formatCols[i])
		upsertSQL.WriteByte(')')
	}

	return upsertSQL.String(), nil
}

func (orm *ORM) mysqlUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
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
				subVal.WriteString(util.Any2String(v))
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
	}
	upsertSQL.WriteByte('(')
	upsertSQL.WriteString(util.JoinArr(formatCols, ","))
	upsertSQL.WriteString(") values")
	upsertSQL.WriteString(util.JoinArr(valArr, ","))

	upsertSQL.WriteString(" on duplicate key update ")

	for i := 0; i < len(formatCols); i++ {
		if i > 0 {
			upsertSQL.WriteByte(',')
		}
		upsertSQL.WriteString(formatCols[i])
		upsertSQL.WriteString("=values(")
		upsertSQL.WriteString(formatCols[i])
		upsertSQL.WriteByte(')')
	}

	return upsertSQL.String(), nil
}
