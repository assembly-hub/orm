package orm

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

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
	Tb   *Table3 `json:"tb3" ref:"left;ref=id"`
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
	// mysql mariadb sqlserver postgres opengauss sqllite oracle
	// mysql mariadb sqlserver postgres opengauss sqllite oracle
	ref = NewReference(dbtype.SQLServer)
	// 添加表定义
	ref.AddTableDef("table1", Table1{})
	ref.AddTableDef("table2", Table2{})
	ref.AddTableDef("table3", Table3{})
	// 编译表关系
	ref.BuildRefs()
}

func TestORM(t *testing.T) {
	var db *sql.DB = &sql.DB{}
	orm := NewORM(context.Background(), "table1", db, ref)
	_, err := orm.ReplaceManySameClos([]interface{}{map[string]interface{}{
		"id":   1,
		"name": "test ORM",
	}, map[string]interface{}{
		"id":   2,
		"name": "test ORM2",
	}}, []string{"id", "name"}, 10, true)

	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestORM2(t *testing.T) {
	var db *sql.DB = &sql.DB{}
	orm := NewORM(context.Background(), "table1", db, ref)

	_, err := orm.ReplaceOne(map[string]interface{}{
		"id":   1,
		"name": "test ORM",
	})
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestORM3(t *testing.T) {
	var db *sql.DB = &sql.DB{}
	orm := NewORM(context.Background(), "table1", db, ref)

	orm.Wheres(Where{
		"id__gt":           0,      // table1的id > 1
		"name__startswith": "test", // table1的name like 'test%'
		// tb2：table1的tag字段（可以理解为table2的别名），tb3：table2的tag字段（可以理解为table3的别名）
		// 含义：select * from table1 left join table2 left join table3 where table3.id > 1
		"tb2.tb3.id__gt": 1,
		"dt__date":       "2023-01-01",
	})
	orm.Page(1, 10)
	fmt.Println(orm.ToSQL(false))
}
