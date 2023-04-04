package orm

import (
	"github.com/assembly-hub/orm/dbtype"
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
	ref = NewReference(dbtype.MySQL)
	// 添加表定义
	ref.AddTableDef("table1", Table1{})
	ref.AddTableDef("table2", Table2{})
	ref.AddTableDef("table3", Table3{})
	// 编译表关系
	ref.BuildRefs()
}
