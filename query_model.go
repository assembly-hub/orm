// package orm
package orm

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/assembly-hub/basics/util"
)

type selectModel struct {
	Table string
	Cols  []string
}

type orderModel struct {
	Table string
	Cols  []string
}

type joinModel struct {
	Type      joinType
	MainTable string
	MainAlias string
	JoinTable string
	JoinAlias string
	On        [][2]string
}

type queryModel struct {
	MainTable       string
	MainAlias       string
	Distinct        bool
	SelectForUpdate bool
	Select          []*selectModel
	Order           []*orderModel
	Limit           []uint
	Where           map[string]interface{}
	JoinList        []*joinModel
	GroupBy         []string
	Having          map[string]interface{}
}

func (p *queryModel) selectSQL() string {
	sql := ""
	for _, sel := range p.Select {
		for _, col := range sel.Cols {
			if sel.Table != "" {
				col = sel.Table + "." + col
			}
			if sql == "" {
				sql = col
			} else {
				sql += "," + col
			}
		}
	}

	if sql == "" {
		sql = "*"
	}

	if p.Distinct {
		sql = "distinct " + sql
	}
	return sql
}

func (p *queryModel) joinSQL() string {
	sql := ""
	for _, join := range p.JoinList {
		if join.Type.value() != "" {
			join.Type += " "
		}
		if join.JoinAlias != "" {
			sql += " " + join.Type.value() + "join " + join.JoinTable + " as " + join.JoinAlias
		} else {
			sql += " " + join.Type.value() + "join " + join.JoinTable
		}

		if join.Type.isNatural() {
			continue
		}
		where := ""
		mt := join.MainTable
		if join.MainAlias != "" {
			mt = join.MainAlias
		}
		jt := join.JoinTable
		if join.JoinAlias != "" {
			jt = join.JoinAlias
		}
		for _, sel := range join.On {
			if where == "" {
				where = mt + "." + sel[0] + "=" + jt + "." + sel[1]
			} else {
				where += " and " + mt + "." + sel[0] + "=" + jt + "." + sel[1]
			}
		}
		if where != "" {
			sql += " on " + where
		}
	}

	return sql
}

func (p *queryModel) orderSQL() string {
	sql := ""
	for _, sel := range p.Order {
		for _, col := range sel.Cols {
			if col[0] == '-' {
				col = col[1:] + " desc"
			} else if col[0] == '+' {
				col = col[1:] + " asc"
			} else {
				col = col + " asc"
			}

			if sel.Table != "" {
				col = sel.Table + "." + col
			}
			if sql == "" {
				sql = col
			} else {
				sql += "," + col
			}
		}
	}

	return sql
}

func (p *queryModel) orSQL(where map[string]interface{}) string {
	return p.whereSQL(where, " or ")
}

func (p *queryModel) andSQL(where map[string]interface{}) string {
	return p.whereSQL(where, " and ")
}

func (p *queryModel) formatOrContainsSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "or_icontains":
		if rawVal != "" {
			subSQL = colName + " like " + "'%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like " + "'%" + v + "%'"
				} else {
					subSQL += " or " + colName + " like " + "'%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "or_contains":
		if rawVal != "" {
			subSQL = colName + " like binary " + "'%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like binary " + "'%" + v + "%'"
				} else {
					subSQL += " or " + colName + " like binary " + "'%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	default:
		panic(fmt.Sprintf("col[%s] operation[%s] no definition", colName, colOperator))
	}

	return subSQL
}

func (p *queryModel) formatOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "or_istartswith":
		if rawVal != "" {
			subSQL = colName + " like " + "'" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like " + "'" + v + "%'"
				} else {
					subSQL += " or " + colName + " like " + "'" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "or_startswith":
		if rawVal != "" {
			subSQL = colName + " like binary " + "'" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like binary " + "'" + v + "%'"
				} else {
					subSQL += " or " + colName + " like binary " + "'" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "or_iendswith":
		if rawVal != "" {
			subSQL = colName + " like " + "'%" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like " + "'%" + v + "'"
				} else {
					subSQL += " or " + colName + " like " + "'%" + v + "'"
				}
			}
			subSQL += ")"
		}
	case "or_endswith":
		if rawVal != "" {
			subSQL = colName + " like binary " + "'%" + rawVal + "'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like binary " + "'%" + v + "'"
				} else {
					subSQL += " or " + colName + " like binary " + "'%" + v + "'"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.formatOrContainsSQL(colOperator, colName, val, rawVal, rawStrArr)
	}

	return subSQL
}

func (p *queryModel) formatLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	subSQL := ""
	switch colOperator {
	case "istartswith":
		if rawVal != "" {
			subSQL = colName + " like " + "'" + rawVal + "%'"
		}
	case "startswith":
		if rawVal != "" {
			subSQL = colName + " like binary " + "'" + rawVal + "%'"
		}
	case "iendswith":
		if rawVal != "" {
			subSQL = colName + " like " + "'%" + rawVal + "'"
		}
	case "endswith":
		if rawVal != "" {
			subSQL = colName + " like binary " + "'%" + rawVal + "'"
		}
	case "icontains":
		if rawVal != "" {
			subSQL = colName + " like " + "'%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like " + "'%" + v + "%'"
				} else {
					subSQL += " and " + colName + " like " + "'%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	case "contains":
		if rawVal != "" {
			subSQL = colName + " like binary " + "'%" + rawVal + "%'"
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL == "" {
					subSQL = "(" + colName + " like binary " + "'%" + v + "%'"
				} else {
					subSQL += " and " + colName + " like binary " + "'%" + v + "%'"
				}
			}
			subSQL += ")"
		}
	default:
		subSQL = p.formatOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}

	return subSQL
}

func (p *queryModel) formatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	subSQL := ""
	switch colOperator {
	case "eq":
		subSQL = colName + "=" + val
	case "bin_eq":
		subSQL = "binary " + colName + "=" + val
	case "between":
		subSQL = colName + " between " + val
	case "lt":
		subSQL = colName + "<" + val
	case "bin_lt":
		subSQL = "binary " + colName + "<" + val
	case "lte":
		subSQL = colName + "<=" + val
	case "bin_lte":
		subSQL = "binary " + colName + "<=" + val
	case "gt":
		subSQL = colName + ">" + val
	case "bin_gt":
		subSQL = "binary " + colName + ">" + val
	case "gte":
		subSQL = colName + ">=" + val
	case "bin_gte":
		subSQL = "binary " + colName + ">=" + val
	case "ne":
		subSQL = colName + "<>" + val
	case "bin_ne":
		subSQL = "binary " + colName + "<>" + val
	case "in":
		subSQL = colName + " in " + val
	case "bin_in":
		subSQL = "binary " + colName + " in " + val
	case "nin":
		subSQL = colName + " not in " + val
	case "bin_nin":
		subSQL = "binary " + colName + " not in " + val
	case "date":
		subSQL = "date(" + colName + ")=" + val
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

func (p *queryModel) formatTimeValue(colOperator, colName string, colData interface{}) (val string, rawVal string, rawStrArr []string) {
	switch colData := colData.(type) {
	case time.Time:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		rawVal = time2Str(colData)
		val = "'" + rawVal + "'"
		if colOperator == "date" {
			val = "'" + strings.Split(rawVal, " ")[0] + "'"
		}
	case *time.Time:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		rawVal = time2Str(*(colData))
		val = "'" + rawVal + "'"
		if colOperator == "date" {
			val = "'" + strings.Split(rawVal, " ")[0] + "'"
		}
	case []time.Time:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic("between where length must be 2")
			}
			val = fmt.Sprintf("'%s'", time2Str(colData[0])) + " and " +
				fmt.Sprintf("'%s'", time2Str(colData[1]))
		} else {
			for _, v := range colData {
				rawStrArr = append(rawStrArr, time2Str(v))
			}
			val = "(" + connectStrArr(rawStrArr, ",", "'", "'") + ")"
		}
	case []*time.Time:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic("between where length must be 2")
			}
			val = fmt.Sprintf("'%s'", time2Str(*(colData[0]))) + " and " +
				fmt.Sprintf("'%s'", time2Str(*(colData[1])))
		} else {
			for _, v := range colData {
				rawStrArr = append(rawStrArr, time2Str(*v))
			}
			val = "(" + connectStrArr(rawStrArr, ",", "'", "'") + ")"
		}
	}
	return
}

func (p *queryModel) formatSQLValue(colOperator, colName string, colData interface{}) (val string, rawVal string, rawStrArr []string) {
	switch colData := colData.(type) {
	case queryModel:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		rawVal = colData.SQL()
		if rawVal != "" {
			val = "(" + rawVal + ")"
		}
	case *queryModel:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		rawVal = colData.SQL()
		if rawVal != "" {
			val = "(" + rawVal + ")"
		}
	case string:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		rawVal = strings.ReplaceAll(colData, "'", "\\'")
		val = "'" + rawVal + "'"
	case []string:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic("between where length must be 2")
			}
			val = strings.ReplaceAll(colData[0], "'", "\\'") + " and " +
				strings.ReplaceAll(colData[1], "'", "\\'")
		} else {
			for _, v := range colData {
				v = strings.ReplaceAll(v, "'", "\\'")
				rawStrArr = append(rawStrArr, v)
			}
			val = "(" + connectStrArr(rawStrArr, ",", "'", "'") + ")"
		}
	case []interface{}:
		if colOperator == "between" {
			if len(colData) != 2 {
				panic("between where length must be 2")
			}
			val = strings.ReplaceAll(fmt.Sprintf("%v", colData[0]), "'", "\\'") + " and " +
				strings.ReplaceAll(fmt.Sprintf("%v", colData[1]), "'", "\\'")
		} else {
			for _, vv := range colData {
				v := fmt.Sprintf("%v", vv)
				v = strings.ReplaceAll(v, "'", "\\'")
				rawStrArr = append(rawStrArr, v)
			}
			val = "(" + connectStrArr(rawStrArr, ",", "'", "'") + ")"
		}
	case bool:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		if colData {
			val = "1"
		} else {
			val = "0"
		}
		rawVal = val
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		val = fmt.Sprintf("%v", colData)
		rawVal = val
	case []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64:
		slice := reflect.ValueOf(colData)
		if slice.Len() <= 0 {
			panic(fmt.Sprintf("colName:[%s] slice not empty", colName))
		}

		if colOperator == "between" {
			if slice.Len() != 2 {
				panic("between where length must be 2")
			}
			val = fmt.Sprintf("%v", slice.Index(0).Interface()) + " and " +
				fmt.Sprintf("%v", slice.Index(1).Interface())
		} else {
			val = "("
			for i := 0; i < slice.Len(); i++ {
				val += fmt.Sprintf("%v", slice.Index(i).Interface()) + ","
			}

			val = val[0:len(val)-1] + ")"
		}
	case time.Time, *time.Time, []time.Time, []*time.Time:
		val, rawVal, rawStrArr = p.formatTimeValue(colOperator, colName, colData)
	default:
		if colOperator == "between" {
			panic("between where length must be 2")
		}

		rawVal = strings.ReplaceAll(fmt.Sprintf("%v", colData), "'", "\\'")
		val = "'" + rawVal + "'"
	}

	return
}

func (p *queryModel) whereSQL(where map[string]interface{}, linker string) string {
	sql := ""
	for colKey, colData := range where {
		if colKey == "" || colData == nil {
			continue
		}

		not := false
		if colKey[:1] == "~" {
			not = true
			colKey = colKey[1:]
		}

		subSQL := ""
		if colKey == "$or" {
			switch colData := colData.(type) {
			case map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				subSQL = p.orSQL(colData)
				if subSQL != "" {
					subSQL = "(" + subSQL + ")"
				}
			case []map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				var sqlArr []string
				for _, d := range colData {
					if len(d) <= 0 {
						continue
					}

					strSQL := p.andSQL(d)
					if strSQL != "" {
						sqlArr = append(sqlArr, "("+strSQL+")")
					}
				}
				if len(sqlArr) > 0 {
					subSQL = "(" + util.JoinArr(sqlArr, " or ") + ")"
				}
			default:
				panic("or cond error")
			}
		} else if colKey == "$and" {
			switch colData := colData.(type) {
			case map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				subSQL = p.andSQL(colData)
				if subSQL != "" {
					subSQL = "(" + subSQL + ")"
				}
			case []map[string]interface{}:
				if len(colData) <= 0 {
					continue
				}

				var sqlArr []string
				for _, d := range colData {
					if len(d) <= 0 {
						continue
					}

					strSQL := p.andSQL(d)
					if strSQL != "" {
						sqlArr = append(sqlArr, "("+strSQL+")")
					}
				}
				if len(sqlArr) > 0 {
					subSQL = "(" + util.JoinArr(sqlArr, " and ") + ")"
				}
			default:
				panic("and cond error")
			}
		} else {
			i := strings.Index(colKey, "__")
			colName, colOperator := "", ""
			if i < 0 {
				colName = colKey
				colOperator = "eq"
			} else {
				arr := strings.Split(colKey, "__")
				colName = arr[0]
				colOperator = arr[1]
			}

			val, rawVal, rawStrArr := p.formatSQLValue(colOperator, colName, colData)
			subSQL = p.formatSubSQL(colOperator, colName, val, rawVal, rawStrArr, colData)
		}

		if subSQL == "" {
			continue
		}

		if not {
			if subSQL[:1] == "(" {
				subSQL = "(not " + subSQL + ")"
			} else {
				subSQL = "(not (" + subSQL + "))"
			}
		}

		if sql == "" {
			sql = subSQL
		} else {
			sql += linker + subSQL
		}
	}
	return sql
}

func (p *queryModel) GetWhere() string {
	where := p.andSQL(p.Where)
	return where
}

func (p *queryModel) Count() string {
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

	sql = fmt.Sprintf("SELECT COUNT(*) as `c` from (%s) as `count_tb`", sql)
	return sql
}

func (p *queryModel) SQL() string {
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
		sql += fmt.Sprintf(" limit %d,%d", p.Limit[0], p.Limit[1])
	}

	if p.SelectForUpdate {
		sql += " for update"
	}

	return sql
}
