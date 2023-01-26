package orm

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type Table1 struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	// type: 指定字段接收数据类型，目前为固定值：json
	JSON []map[string]interface{} `json:"json" type:"json"`
	// tb：查询tag，跨表查询可以用
	// ref：关联配置，left是关联逻辑；tb_id=id为关联条件，多个逗号隔开
	// 如：left;tb_id=id,name=name，其中tb_id为当前表字段，id为被关联表字段，以此类推
	Tb *Table2 `json:"tb2" ref:"left;ref=id"`
}

type Table2 struct {
	ID   int     `json:"id"`
	Name string  `json:"name"`
	Tb   *Table3 `json:"tb3" ref:"left;tb=id"`
}

type Table3 struct {
	ID   int     `json:"id"`
	Name string  `json:"name"`
	Tb   *Table1 `json:"tb1" ref:"left;ref=id"`
}

var ref *Reference

func init() {
	// 使用之前需要完成表定义
	// 定义表关联
	ref = NewReference()
	// 添加表定义
	ref.AddTableDef("table1", Table1{})
	ref.AddTableDef("table2", Table2{})
	ref.AddTableDef("table3", Table3{})
	// 编译表关系
	ref.BuildRefs()
}

func TestNewORM(t *testing.T) {
	ctx := context.Background()
	var conn = GetMySQL()
	// 创建以 table1 为主表的orm对象
	db := NewORM(ctx, "table1", conn, ref)
	// 查询完成，是否保留历史查询参数
	db.KeepQuery(false)
	// 需要查询的字段，可自定义别名
	// db.Select("name", "tb.name", "tb.tb.name", "tb.sum(*) as c")
	db.Select("name", "tb.name")

	// where 条件 支持子查询
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
	//		"key__istartswith": "123"
	//		"key__startswith": "123"
	//		"key__iendswith": "123"
	//		"key__endswith": "123"
	//		"key__icontains": "123" or ["123", "123"]
	//		"key__contains": "123" or ["123", "123"]
	// or_like :
	//		"key__or_istartswith": "123" or ["123", "123"]
	//		"key__or_startswith": "123" or ["123", "123"]
	//		"key__or_iendswith": "123" or ["123", "123"]
	//		"key__or_endswith": "123" or ["123", "123"]
	//		"key__or_icontains": "123" or ["123", "123"]
	//		"key__or_contains": "123" or ["123", "123"]
	// ~：结果取反
	// 以下三种方式等价
	db.Query("name__istartswith", "123", "id__gt", 1)
	db.Wheres(Where{
		"name__istartswith": "123",
		"id__gt":            1,
	})
	db.Where("name__istartswith", "123").Where("id__gt", 1)

	// 查询取反，实际 id != 1
	db.Where("~id", 1)

	// or：id==1 or name==test
	db.Where("$or", Where{
		"id":   1,
		"name": time.Now(),
	})

	// or：(id==1 and name==test) or (id==2 and name==test2)
	db.Where("$or", []Where{
		{
			"id":   1,
			"name": "test",
		},
		{
			"id":                   2,
			"name__or_istartswith": "test2",
		},
	})

	// # 为内置符号，标志为原始字段，不进行任何处理
	db.Where("#count(id)__gt", 10)
	db.Where("~#count(id)", 20)

	fmt.Println("SQL: ", db.ToSQL(false))

	// 接收数据
	// var data []map[string]interface{}
	// var data []string
	var data []Table1
	// 参数：flat=false，意味着数据以嵌套的方式返回，可以下钻取数据：data.Tb.Tb.Name
	err := db.ToData(&data, false)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(data)
}
