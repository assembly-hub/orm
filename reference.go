// Package orm
package orm

import (
	"fmt"
	"reflect"
	"strings"
)

type referenceData struct {
	FromTable string
	fromAlias string
	Type      joinType
	Tag       string
	tagList   []string
	ToTable   string
	toAlias   string
	On        [][2]string
}

func (ref *referenceData) Copy() *referenceData {
	newObj := new(referenceData)
	*newObj = *ref
	return newObj
}

type tableRefData struct {
	Tag          string
	Join         joinType
	On           [][2]string
	ToStructName string
}

type structField struct {
	JSONName string
	RawName  string
	DataType reflect.Type
	Ref      bool
	Custom   bool
	Offset   uintptr
	Index    int
}

type tableStructData struct {
	FieldMap   map[string]structField
	StructType reflect.Type
}

type Reference struct {
	dbType        int
	dbConf        *dbCoreData
	joinConf      map[string]map[string]*referenceData
	tableDef      map[string][]string
	structToTable map[string]string
	tableRef      map[string][]*tableRefData
	tableCache    map[string]tableStructData
}

type formatColumnData struct {
	FormatCol  string
	TableName  string
	TableAlias string
	TableCol   string
	FuncName   string
	Alias      string
	TagList    []string
}

func NewReference(dbType int) *Reference {
	obj := new(Reference)
	obj.dbType = dbType
	obj.dbConf = dbConfMap[obj.dbType]
	if obj.dbConf == nil {
		panic(fmt.Errorf("database type[%d] not implemented, please confirm", dbType))
	}
	obj.joinConf = map[string]map[string]*referenceData{}
	obj.tableDef = map[string][]string{}
	obj.structToTable = map[string]string{}
	obj.tableRef = map[string][]*tableRefData{}
	obj.tableCache = map[string]tableStructData{}
	return obj
}

func (c *Reference) GetDBType() int {
	return c.dbType
}

func (c *Reference) getTableCacheByTp(tableTp reflect.Type) *tableStructData {
	if tableTp.Kind() == reflect.Ptr {
		tableTp = tableTp.Elem()
	}

	if tableTp.Kind() != reflect.Struct {
		panic("flat=false, field type must be [*]struct")
	}

	structFullName := fmt.Sprintf("%s.%s", tableTp.PkgPath(), tableTp.Name())
	if v, ok := c.tableCache[structFullName]; ok {
		return &v
	}
	return nil
}

func (c *Reference) getTableCacheByPath(tablePath string) *tableStructData {
	if v, ok := c.tableCache[tablePath]; ok {
		return &v
	}
	return nil
}

func (c *Reference) getDBConf() *dbCoreData {
	return c.dbConf
}

func (c *Reference) getTableName(structName string) string {
	return c.structToTable[structName]
}

func (c *Reference) GetTableDef(table string) []string {
	return c.tableDef[table]
}

func computeStructData(tp reflect.Type) (*tableStructData, error) {
	if tp.Kind() == reflect.Ptr {
		tp = tp.Elem()
	}

	if tp.Kind() != reflect.Struct {
		return nil, fmt.Errorf("need struct type")
	}
	tbs := tableStructData{
		StructType: tp,
	}
	tbs.FieldMap = make(map[string]structField)
	for i := 0; i < tp.NumField(); i++ {
		colName := tp.Field(i).Tag.Get("json")
		if colName == "" || !tp.Field(i).IsExported() {
			continue
		}

		ref := tp.Field(i).Tag.Get("ref")
		dataType := tp.Field(i).Tag.Get("type")
		tbs.FieldMap[colName] = structField{
			JSONName: colName,
			RawName:  tp.Field(i).Name,
			DataType: tp.Field(i).Type,
			Ref:      ref != "",
			Custom:   dataType != "",
			Offset:   tp.Field(i).Offset,
			Index:    i,
		}
	}
	return &tbs, nil
}

// AddTableDef 添加表定义
func (c *Reference) AddTableDef(table string, def interface{}) {
	err := globalVerifyObj.VerifyTableName(table)
	if err != nil {
		panic(err)
	}

	if _, ok := c.tableDef[table]; ok {
		panic(fmt.Sprintf("table [%s] is already in def", table))
	}

	tp := reflect.TypeOf(def)
	if tp.Kind() != reflect.Struct {
		panic(fmt.Sprintf("table [%s] type must be struct", table))
	}

	structFullName := fmt.Sprintf("%s.%s", tp.PkgPath(), tp.Name())
	if _, ok := c.structToTable[structFullName]; ok {
		panic(fmt.Sprintf("table [%s] struct [%s] is exist", table, structFullName))
	}
	c.structToTable[structFullName] = table

	var cols []string
	for i := 0; i < tp.NumField(); i++ {
		colName := tp.Field(i).Tag.Get("json")
		if colName == "" || !tp.Field(i).IsExported() {
			continue
		}

		ref := tp.Field(i).Tag.Get("ref")
		if ref != "" {
			err = globalVerifyObj.VerifyTagName(colName)
			if err != nil {
				panic(err)
			}

			refType := tp.Field(i).Type
			if refType.Kind() == reflect.Ptr {
				refType = refType.Elem()
			}

			if refType.Kind() != reflect.Struct {
				panic(fmt.Sprintf("table [%s] ref [%s] type must be [*]struct", table, colName))
			}

			toStructName := fmt.Sprintf("%s.%s", refType.PkgPath(), refType.Name())
			arr := strings.Split(ref, ";")

			if len(arr) < 1 {
				panic(fmt.Sprintf("table[%s] ref data error, must be \"joinType[;id=rid,name=rname,...]\"", table))
			}

			join := toJoinData(arr[0])
			tbRef := &tableRefData{
				Tag:          colName,
				Join:         join,
				On:           nil,
				ToStructName: toStructName,
			}

			if len(arr) == 2 && arr[1] != "" {
				var joinOn [][2]string
				onWhere := strings.Split(arr[1], ",")
				for _, on := range onWhere {
					arr := strings.Split(on, "=")
					if len(arr) < 2 {
						continue
					}
					err = globalVerifyObj.VerifyFieldName(arr[0])
					if err != nil {
						panic(err)
					}
					err = globalVerifyObj.VerifyFieldName(arr[1])
					if err != nil {
						panic(err)
					}
					joinOn = append(joinOn, [2]string{arr[0], arr[1]})
				}
				tbRef.On = joinOn
			}

			c.tableRef[table] = append(c.tableRef[table], tbRef)
		} else {
			err = globalVerifyObj.VerifyFieldName(colName)
			if err != nil {
				panic(err)
			}
			cols = append(cols, colName)
		}
	}
	if len(cols) <= 0 {
		panic(fmt.Sprintf("table [%s] have no fields", table))
	}
	c.tableDef[table] = cols
	tps, err := computeStructData(tp)
	if err != nil {
		panic(err)
	}
	c.tableCache[structFullName] = *tps
}

// BuildRefs 构建数据表关系，此操作必须在表定义完成时调用
func (c *Reference) BuildRefs() {
	if len(c.tableDef) <= 0 && len(c.structToTable) <= 0 {
		return
	}

	if len(c.tableDef) != len(c.structToTable) {
		panic("table def data error")
	}

	for tableName, refArr := range c.tableRef {
		for _, ref := range refArr {
			toTableName := c.structToTable[ref.ToStructName]
			if toTableName == "" {
				panic(fmt.Sprintf("table[%s] tag[%s] to table name error", tableName, ref.Tag))
			}

			c.addReference(&referenceData{
				FromTable: tableName,
				Type:      ref.Join,
				Tag:       ref.Tag,
				ToTable:   toTableName,
				On:        ref.On,
			})
		}
	}
}

func (c *Reference) getLevelCols(table string, tag string, level int) []string {
	if level < 0 {
		return nil
	}

	prefix := tag
	if tag != "" {
		table = c.getTableByMainTableAndTag(table, tag)
	}

	var cols []string
	needHandleTable := [][3]interface{}{{table, prefix, level}}
	for {
		if len(needHandleTable) <= 0 {
			break
		}

		tb := needHandleTable[0]
		cols = append(cols, c.getTableTagCols(tb[0].(string), tb[1].(string))...)
		needHandleTable = needHandleTable[1:]

		if tb[2].(int) > 0 {
			tags := c.getTableTagList(tb[0].(string), tb[1].(string), tb[2].(int)-1)
			needHandleTable = append(needHandleTable, tags...)
		}
	}

	return cols
}

func (c *Reference) getTableByMainTableAndTag(mainTable string, tag string) string {
	tags := strings.Split(tag, ".")
	if len(tags) <= 0 {
		panic(fmt.Sprintf("table[%s] tag[%s] error", mainTable, tag))
	}

	tb := mainTable
	for _, p := range tags {
		ref := c.joinConf[tb][p]
		if ref == nil {
			panic(fmt.Sprintf("table[%s] tag[%s] error", mainTable, tag))
		}

		tb = ref.ToTable
	}
	return tb
}

func (c *Reference) getTableTagList(table string, parentTag string, level int) [][3]interface{} {
	var tagList [][3]interface{}
	for _, v := range c.joinConf[table] {
		if parentTag != "" {
			tagList = append(tagList, [3]interface{}{v.ToTable, fmt.Sprintf("%s.%s", parentTag, v.Tag), level})
		} else {
			tagList = append(tagList, [3]interface{}{v.ToTable, v.Tag, level})
		}
	}
	return tagList
}

func (c *Reference) getTableTagCols(table string, prefix string) []string {
	cols := c.GetTableDef(table)
	if len(cols) <= 0 {
		panic(fmt.Sprintf("table[%s] unregistered", table))
	}
	selectList := make([]string, len(cols))
	if prefix != "" {
		for i := range cols {
			selectList[i] = fmt.Sprintf("%s.%s", prefix, cols[i])
		}
	} else {
		copy(selectList, cols)
	}

	return selectList
}

func (c *Reference) getJoinData(table, tag string) *referenceData {
	return c.joinConf[table][tag]
}

func (c *Reference) addConf(table, tag string, conf *referenceData) {
	obj := c.joinConf[table]
	if obj == nil {
		obj = map[string]*referenceData{}
	}

	obj[tag] = conf
	c.joinConf[table] = obj
}

func (c *Reference) addReference(confList ...*referenceData) *Reference {
	for _, conf := range confList {
		obj := c.getJoinData(conf.FromTable, conf.Tag)
		if obj != nil {
			panic(fmt.Sprintf("table:%s, tag:%s is already exist", conf.FromTable, conf.Tag))
		}
		c.addConf(conf.FromTable, conf.Tag, conf)
	}
	return c
}
