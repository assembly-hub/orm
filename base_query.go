// package orm
package orm

import (
	"fmt"
	"strings"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

const (
	defaultAliasPrefix = "orm_"
)

// where 条件 支持子查询 BaseQuery or *BaseQuery
// bin含义：区分文本大小
// = : "key": "val" or "key__eq": "val" or "key__bin_eq": "val"
// < : "key__lt": 1 or "key__bin_lt": 1
// <= : "key__lte": 1 or "key__bin_lte": 1
// > : "key__gt": 1 or "key__bin_gt": 1
// >= : "key__gte": 1 or "key__bin_gte": 1
// != : "key__ne": 1 or "key__bin_ne": 1
// in : "key__in": [1] or "key__bin_in": [1]
// not in : "key__nin": [1] or "key__bin_nin": [1]
// date : "key__date": "2022-01-01"
// between : "key__between": [1, 2]

// 以下不支持子查询
// is null : "key__null": true
// is not null : "key__null": false
// $or : map[string]interface{} or []map[string]interface{}
// $and : map[string]interface{} or []map[string]interface{}
// and_like :
//		"key__startswith": "123"
//		"key__endswith": "123"
//		"key__contains": "123" or ["123", "123"]
// or_like :
//		"key__or_startswith": "123" or ["123", "123"]
//		"key__or_endswith": "123" or ["123", "123"]
//		"key__or_contains": "123" or ["123", "123"]

// BaseQuery
// # 为内置符号，标志为原始字段，不进行任何处理，仅在以下数据有效：
// Select Order GroupBy Where Having
type BaseQuery struct {
	// 用户自定义sql
	CustomSQL  string
	PrivateKey string
	RefConf    *Reference
	joinSet    set.Set[string]
	tagSet     set.Set[string]
	joinLevel  [][]*referenceData
	// 字段别名链接字符串，SelectRaw 为false有效
	SelectColLinkStr string
	// true：使用原始字段名；false：使用别名
	SelectRaw bool

	TableName       string
	Distinct        bool
	SelectForUpdate bool
	Select          Select
	Order           Order
	Limit           Limit
	Where           Where
	GroupBy         GroupBy
	Having          Having
}

func (q *BaseQuery) initJoinData() {
	q.joinSet = set.Set[string]{}
	q.tagSet = set.Set[string]{}
	q.joinLevel = nil
}

func (q *BaseQuery) addTag(tag string) {
	q.tagSet.Add(tag)
}

func (q *BaseQuery) addRef(level int, ref *referenceData) {
	key := fmt.Sprintf("%s-%s", ref.FromTable, util.JoinArr(ref.tagList, "_"))
	if q.joinSet.Has(key) {
		return
	}
	q.joinSet.Add(key)

	size := len(q.joinLevel)
	if size <= level {
		size = level + 1 - size
		for i := 0; i < size; i++ {
			q.joinLevel = append(q.joinLevel, []*referenceData{})
		}
	}

	q.joinLevel[level] = append(q.joinLevel[level], ref)
}

func (q *BaseQuery) formatColumn(sel string) *formatColumnData {
	colData := &formatColumnData{}

	dbCore := q.RefConf.getDBConf()

	tagTable := q.TableName
	col := sel

	// 检查跨表字段
	colArr := strings.Split(sel, ".")
	tagJoinStr := ""
	if len(colArr) > 1 {
		q.addTag(util.JoinArr(colArr[:len(colArr)-1], "."))
		prefixTable := q.TableName
		if q.RefConf == nil {
			panic("orm reference is nil")
		}

		for level, tag := range colArr[:len(colArr)-1] {
			ref := q.RefConf.getJoinData(prefixTable, tag)
			if ref == nil {
				panic(fmt.Sprintf("ref error, table[%s] tag[%s] not exist", prefixTable, tag))
			}

			colData.TagList = append(colData.TagList, tag)

			newRef := ref.Copy()

			newRef.tagList = colArr[:level+1]
			if level >= 0 {
				newRef.toAlias = fmt.Sprintf("%s%s%s%s", dbCore.EscStart, defaultAliasPrefix,
					util.JoinArr(colArr[:level+1], "_"), dbCore.EscEnd)
				if level >= 1 {
					newRef.fromAlias = fmt.Sprintf("%s%s%s%s", dbCore.EscStart, defaultAliasPrefix,
						util.JoinArr(colArr[:level], "_"), dbCore.EscEnd)
				}
			}

			prefixTable = ref.ToTable
			tagTable = ref.ToTable

			q.addRef(level, newRef)
		}
		tagJoinStr = fmt.Sprintf("%s%s", defaultAliasPrefix, util.JoinArr(colArr[:len(colArr)-1], "_"))
		col = colArr[len(colArr)-1]
	}

	oldLen := len(col)
	col = string(util.CharMerge(col, byte(' ')))
	if oldLen != len(col) {
		panic(fmt.Sprintf("field[%s] there are extra spaces", col))
	}
	if col == "" || col[0] == ' ' || col[len(col)-1] == ' ' {
		panic(fmt.Sprintf("field[%s] illegal characters of letter of guarantee", col))
	}

	colData.TableName = tagTable

	if tagJoinStr != "" {
		tagTable = tagJoinStr
	}

	colData.TableAlias = tagTable
	// 检查数据库函数
	i, j := strings.Index(col, "("), strings.Index(col, ")")
	if i > 0 && j > i {
		action := col[:i]
		colName := col[i+1 : j]

		if action == "" {
			panic(fmt.Sprintf("field[%s] function error", col))
		}

		if colName == "" {
			panic(fmt.Sprintf("field[%s] error", col))
		}

		colData.TableCol = colName
		colData.FuncName = action
		colData.Alias = col[j+1:]

		colData.FormatCol = fmt.Sprintf("%s(%s%s%s.%s%s%s)%s", action,
			dbCore.EscStart, tagTable, dbCore.EscEnd,
			dbCore.EscStart, colData.TableCol, dbCore.EscEnd, colData.Alias)
		if colData.TableCol == "*" {
			colData.FormatCol = fmt.Sprintf("%s(%s%s%s.%s)%s", action,
				dbCore.EscStart, tagTable, dbCore.EscEnd,
				colData.TableCol, colData.Alias)
		} else {
			err := globalVerifyObj.VerifyFieldName(colData.TableCol)
			if err != nil {
				panic(err)
			}
		}
		return colData
	}

	colData.FuncName = ""
	colData.Alias = ""
	i = strings.Index(col, " ")
	colData.TableCol = col
	colData.FormatCol = fmt.Sprintf("%s%s%s.%s%s%s",
		dbCore.EscStart, tagTable, dbCore.EscEnd,
		dbCore.EscStart, col, dbCore.EscEnd)
	if i > 0 {
		colData.TableCol = col[:i]
		err := globalVerifyObj.VerifyFieldName(colData.TableCol)
		if err != nil {
			panic(err)
		}

		colData.Alias = col[i+1:]
		colData.FormatCol = fmt.Sprintf("%s%s%s.%s%s%s %s",
			dbCore.EscStart, tagTable, dbCore.EscEnd,
			dbCore.EscStart, colData.TableCol, dbCore.EscEnd,
			colData.Alias)
	} else if col == "*" {
		colData.TableCol = col
		colData.FormatCol = fmt.Sprintf("%s%s%s.%s",
			dbCore.EscStart, tagTable, dbCore.EscEnd,
			colData.TableCol)
	}

	return colData
}

// 解包select字段
func (q *BaseQuery) selectUnzip() {
	if len(q.Select) <= 0 {
		return
	}

	needRemoveCol := set.Set[string]{}
	var cols []string
	for _, sel := range q.Select {
		if sel[0] == '#' {
			cols = append(cols, sel)
			continue
		}

		exclude := false
		if sel[0] == '-' {
			exclude = true
			sel = sel[1:]
		}

		var unZipCol []string
		if i := strings.Index(sel, "*"); i >= 0 && (i == 0 || sel[i-1] == '.') {
			num := sel[i+1:]
			var level int
			var err error
			if num == "" {
				level = 0
			} else {
				level, err = util.Str2Int[int](num)
			}
			if err != nil {
				panic(err)
			}
			tag := ""
			if i > 0 {
				tag = sel[:i-1]
			}

			unZipCol = q.RefConf.getLevelCols(q.TableName, tag, level)
		} else {
			unZipCol = []string{sel}
		}

		if exclude {
			needRemoveCol.Add(unZipCol...)
		} else {
			cols = append(cols, unZipCol...)
		}
	}
	var realCols []string
	realCol := set.New[string]()
	for _, col := range cols {
		if needRemoveCol.Has(col) {
			continue
		}

		if !realCol.Has(col) {
			realCols = append(realCols, col)
			realCol.Add(col)
		}
	}
	if len(realCols) <= 0 {
		panic("the fields cannot be all deleted or are originally empty")
	}
	q.Select = realCols
}

func (q *BaseQuery) aliasSelectData() []*selectModel {
	dbCore := q.RefConf.getDBConf()
	q.selectUnzip()
	if len(q.Select) <= 0 {
		return nil
	}

	linkStr := "_"
	if q.SelectColLinkStr != "" {
		linkStr = q.SelectColLinkStr
	}

	selObj := &selectModel{}

	for _, sel := range q.Select {
		if sel[0] == '#' {
			selObj.Cols = append(selObj.Cols, sel[1:])
		} else {
			colData := q.formatColumn(sel)
			tagLabel := util.JoinArr(colData.TagList, linkStr)

			if colData.Alias != "" {
				selObj.Cols = append(selObj.Cols, colData.FormatCol)
			} else if colData.FuncName != "" {
				txt := colData.TableCol
				if len(colData.TagList) <= 0 {
					if colData.TableCol == "*" {
						txt = colData.TableAlias
					}
					selObj.Cols = append(selObj.Cols, fmt.Sprintf("%s as %s%s%s%s%s", colData.FormatCol,
						dbCore.EscStart, txt, linkStr, colData.FuncName, dbCore.EscEnd))
				} else {
					if colData.TableCol == "*" {
						txt = linkStr
					} else {
						txt = linkStr + txt + linkStr
					}
					selObj.Cols = append(selObj.Cols, fmt.Sprintf("%s as %s%s%s%s%s", colData.FormatCol,
						dbCore.EscStart, tagLabel, txt, colData.FuncName, dbCore.EscEnd))
				}
			} else if colData.TableCol == "*" {
				if q.RefConf == nil {
					selObj.Cols = append(selObj.Cols, colData.FormatCol)
				} else {
					cols := q.RefConf.GetTableDef(colData.TableName)
					if len(cols) > 0 {
						if len(colData.TagList) <= 0 {
							for _, col := range cols {
								selObj.Cols = append(selObj.Cols, fmt.Sprintf("%s%s%s.%s%s%s",
									dbCore.EscStart, colData.TableAlias, dbCore.EscEnd,
									dbCore.EscStart, col, dbCore.EscEnd))
							}
						} else {
							for _, col := range cols {
								selObj.Cols = append(selObj.Cols, fmt.Sprintf("%s%s%s.%s%s%s as %s%s%s%s%s",
									dbCore.EscStart, colData.TableAlias, dbCore.EscEnd,
									dbCore.EscStart, col, dbCore.EscEnd,
									dbCore.EscStart, tagLabel, linkStr, col, dbCore.EscEnd))
							}
						}
					} else {
						selObj.Cols = append(selObj.Cols, colData.FormatCol)
					}
				}
			} else {
				if len(colData.TagList) <= 0 {
					selObj.Cols = append(selObj.Cols, colData.FormatCol)
				} else {
					selObj.Cols = append(selObj.Cols, fmt.Sprintf("%s as %s%s%s%s%s", colData.FormatCol,
						dbCore.EscStart, tagLabel, linkStr, colData.TableCol, dbCore.EscEnd))
				}
			}
		}
	}

	return []*selectModel{selObj}
}

func (q *BaseQuery) rawSelectData() []*selectModel {
	if len(q.Select) <= 0 {
		return nil
	}

	selObj := &selectModel{}

	for _, sel := range q.Select {
		if sel[0] == '#' {
			selObj.Cols = append(selObj.Cols, sel[1:])
		} else {
			colData := q.formatColumn(sel)
			selObj.Cols = append(selObj.Cols, colData.FormatCol)
		}
	}

	return []*selectModel{selObj}
}

func (q *BaseQuery) selectData() []*selectModel {
	if !q.SelectRaw {
		return q.aliasSelectData()
	}

	return q.rawSelectData()
}

func (q *BaseQuery) orderData() []*orderModel {
	if len(q.Order) <= 0 {
		return nil
	}

	orderObj := &orderModel{}

	for _, sel := range q.Order {
		prefix := ""
		if sel[0] == '-' || sel[1] == '+' {
			prefix = sel[:1]
			sel = sel[1:]
		}

		if sel[0] == '#' {
			orderObj.Cols = append(orderObj.Cols, prefix+sel[1:])
		} else {
			colData := q.formatColumn(sel)
			orderObj.Cols = append(orderObj.Cols, prefix+colData.FormatCol)
		}
	}

	return []*orderModel{orderObj}
}

func (q *BaseQuery) groupData() []string {
	if len(q.GroupBy) <= 0 {
		return nil
	}

	cols := make([]string, 0, len(q.GroupBy))
	for _, sel := range q.GroupBy {
		if sel[0] == '#' {
			cols = append(cols, sel[1:])
		} else {
			colData := q.formatColumn(sel)
			cols = append(cols, colData.FormatCol)
		}
	}

	return cols
}

func (q *BaseQuery) formatCond(where map[string]interface{}) map[string]interface{} {
	newCond := map[string]interface{}{}
	for k, v := range where {
		not := ""
		if k[:1] == "~" {
			not = "~"
			k = k[1:]
		}

		val := v
		switch v := v.(type) {
		case BaseQuery:
			val = v.cond()
		case *BaseQuery:
			val = v.cond()
		case map[string]interface{}:
			if len(v) <= 0 {
				continue
			}
			val = q.formatCond(v)
		case []map[string]interface{}:
			if len(v) <= 0 {
				continue
			}
			val = []map[string]interface{}{}
			ok := false
			for _, query := range v {
				c := q.formatCond(query)
				if len(c) > 0 {
					ok = true
					val = append(val.([]map[string]interface{}), c)
				}
			}
			if !ok {
				continue
			}
		}

		if k[0] == '#' {
			newCond[not+k[1:]] = val
		} else if k == "$or" || k == "$and" {
			newCond[not+k] = val
		} else {
			arr := strings.Split(k, "__")
			if len(arr) <= 1 {
				colData := q.formatColumn(k)
				newCond[not+colData.FormatCol] = val
			} else {
				colData := q.formatColumn(arr[0])
				newCond[not+fmt.Sprintf("%s__%s", colData.FormatCol, arr[1])] = val
			}
		}
	}
	return newCond
}

func (q *BaseQuery) formatWhere() map[string]interface{} {
	return q.formatCond(q.Where)
}

func (q *BaseQuery) formatHaving() map[string]interface{} {
	if len(q.GroupBy) > 0 {
		return q.formatCond(q.Having)
	}
	return nil
}

func (q *BaseQuery) formatJoin() []*joinModel {
	if len(q.joinLevel) <= 0 {
		return nil
	}

	dbCore := q.RefConf.getDBConf()

	joinArr := make([]*joinModel, 0, len(q.joinSet))
	for _, levelData := range q.joinLevel {
		for _, ref := range levelData {
			on := make([][2]string, 0, len(ref.On))
			for _, v := range ref.On {
				on = append(on, [2]string{
					fmt.Sprintf("%s%s%s", dbCore.EscStart, v[0], dbCore.EscEnd),
					fmt.Sprintf("%s%s%s", dbCore.EscStart, v[1], dbCore.EscEnd),
				})
			}

			temp := &joinModel{
				Type:      ref.Type,
				MainTable: fmt.Sprintf("%s%s%s", dbCore.EscStart, ref.FromTable, dbCore.EscEnd),
				MainAlias: ref.fromAlias,
				JoinTable: fmt.Sprintf("%s%s%s", dbCore.EscStart, ref.ToTable, dbCore.EscEnd),
				JoinAlias: ref.toAlias,
				On:        on,
			}
			joinArr = append(joinArr, temp)
		}
	}
	return joinArr
}

func (q *BaseQuery) cond() *queryModel {
	if q.TableName == "" {
		panic("table name error")
	}

	err := globalVerifyObj.VerifyTableName(q.TableName)
	if err != nil {
		panic(err)
	}

	q.initJoinData()

	dbCore := q.RefConf.getDBConf()

	mainTableName := fmt.Sprintf("%s%s%s", dbCore.EscStart, q.TableName, dbCore.EscEnd)

	query := &queryModel{
		PrivateKey:      q.PrivateKey,
		DBCore:          dbCore,
		MainTable:       mainTableName,
		MainAlias:       "",
		Distinct:        q.Distinct,
		SelectForUpdate: q.SelectForUpdate,
		Limit:           q.Limit,
		Select:          q.selectData(),
		Order:           q.orderData(),
		GroupBy:         q.groupData(),
		Where:           q.formatWhere(),
		Having:          q.formatHaving(),
		JoinList:        q.formatJoin(),
	}

	if !q.SelectRaw && len(q.Select) <= 0 {
		tagList := q.tagSet.ToList()
		q.Select = []string{"*"}
		for _, tag := range tagList {
			q.Select = append(q.Select, fmt.Sprintf("%s.*", tag))
		}
		query.Select = q.selectData()
	}

	return query
}

func (q *BaseQuery) Count() string {
	return q.cond().Count()
}

func (q *BaseQuery) GetWhere() string {
	return q.cond().GetWhere()
}

func (q *BaseQuery) SQL() string {
	if q.CustomSQL != "" {
		return q.CustomSQL
	}
	return q.cond().SQL()
}
