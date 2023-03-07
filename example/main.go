package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/assembly-hub/orm"
	"github.com/assembly-hub/orm/example/dao"
)

var mysqlRef = orm.NewReference()

func initRef() {
	// 表定义：建议放在表结构文件的init函数中
	mysqlRef.AddTableDef("table1", dao.Table1{})
	mysqlRef.AddTableDef("table2", dao.Table2{})
	mysqlRef.AddTableDef("table3", dao.Table3{})
	mysqlRef.BuildRefs()
}

func getMySQL() *sql.DB {
	cli := orm.NewClient(&orm.Config{
		Host:            "127.0.0.1",
		Port:            3306,
		Username:        "root",
		Password:        "root",
		DBDriver:        "mysql",
		DBName:          "example",
		MaxOpenConn:     10,
		MaxIdleConn:     10,
		ConnMaxLifeTime: 5000,
		DSNParams:       "charset=utf8&timeout=90s&readTimeout=5s&writeTimeout=5s&collation=utf8mb4_unicode_ci&parseTime=true",
	})
	db, err := cli.Connect()
	if err != nil {
		panic(err)
	}
	return db
}

func main() {
	// 初始化表定义
	initRef()

	// 初始化数据库链接
	db := getMySQL()

	ctx := context.Background()

	tb1 := orm.NewORM(ctx, "table1", db, mysqlRef)
	// 条件查询
	tb1.Query("id__gt", 1, "name__istartswith", "test", "tb2.tb3.id__gt", 1)
	// 等价
	tb1.Wheres(orm.Where{
		"id__gt":            1,      // table1的id > 1
		"name__istartswith": "test", // table1的name like 'test%'
		// tb2：table1的tag字段（可以理解为table2的别名），tb3：table2的tag字段（可以理解为table3的别名）
		// 含义：select * from table1 left join table2 left join table3 where table3.id > 1
		"tb2.tb3.id__gt": 1,
	})

	exist, err := tb1.Exist()
	if err != nil {
		fmt.Println(exist, err.Error())
	}
	/**
	where 条件 支持子查询 MySqlQuery or *MySqlQuery
	= : "key": "val" or "key__eq": "val" or "key__bin_eq": "val"
	< : "key__lt": 1 or "key__bin_lt": 1
	<= : "key__lte": 1 or "key__bin_lte": 1
	> : "key__gt": 1 or "key__bin_gt": 1
	>= : "key__gte": 1 or "key__bin_gte": 1
	!= : "key__ne": 1 or "key__bin_ne": 1
	in : "key__in": [1] or "key__bin_in": [1]
	not in : "key__nin": [1] or "key__bin_nin": [1]
	date : "key__date": "2022-01-01"
	between : "key__between": [1, 2]

	以下不支持子查询
	is null : "key__null": true
	is not null : "key__null": false
	$or : map[string]interface{} or []map[string]interface{}
	$and : map[string]interface{} or []map[string]interface{}
	and_like :
			"key__istartswith": "123"
			"key__startswith": "123"
			"key__iendswith": "123"
			"key__endswith": "123"
			"key__icontains": "123" or ["123", "123"]
			"key__contains": "123" or ["123", "123"]
	or_like :
			"key__or_istartswith": "123" or ["123", "123"]
			"key__or_startswith": "123" or ["123", "123"]
			"key__or_iendswith": "123" or ["123", "123"]
			"key__or_endswith": "123" or ["123", "123"]
			"key__or_icontains": "123" or ["123", "123"]
			"key__or_contains": "123" or ["123", "123"]

	原始数据，#修饰的字段为原始字段，不做处理，其他的字段会根据tag计算
	~ 为条件取反，必须在最前面，可用在所有算子前面，如果与#连用，#应在~后面，如：~#test
	*/

	// 查询数据
	var data dao.Table1
	// 第一个参数：接收数据的容器，可以是 struct 或 map 或 []map 或 []struct
	// 		参数非slice时，查询会只关心第一条
	// 第二个参数：false，子表数据查出之后会以嵌套的方式写入到第一个参数
	/** 例子
	{
		"id": 1,
		"name": "test",
		"json": ["test1", "test2", "test3"],
		"ref": 1,
		"tb2": {
			"id": 1,
			"name": "test",
			"age": 1,
			"ref": 0,
			"tb3": null
		}
	}
	*/
	// 		参数：true，会根据 SelectColLinkStr 指定的链接串，默认是 _ ，将tag数据组装，写到第一个参数中
	/** 例子
	{
		"id": 1,
		"name": "test",
		"json": ["test1", "test2", "test3"],
		"ref": 1,
		"tb2_id": 1,
		"tb2_name": "test",
		"tb2_age": 1,
		"tb2_ref": 0
	}
	*/
	err = tb1.ToData(&data, false)
	if err != nil {
		fmt.Println(err)
	}

	dt := map[string]interface{}{
		"name": "test",
		"json": []string{"1", "2", "3"}, // 框架会进行处理
	}
	// 参数可以是 map 或 table1 struct，当id == nil or id == "" or id == 0 将忽略id字段，否则将插入
	_, err = tb1.InsertOne(dt)
	if err != nil {
		fmt.Println(err)
	}

	insertList := []interface{}{}
	// 参数1：需要插入的数据列表，元素可以是 map 或 table1 struct，其他和 InsertOne 一致
	// 参数2：是否采用事务的方式，true采用事务，false不采用
	_, _, err = tb1.InsertMany(insertList, true)
	if err != nil {
		fmt.Println(err)
	}

	updateData := map[string]interface{}{
		"id":   1, // 更新条件，需要主键或者唯一索引
		"name": "test",
		"json": []string{"1", "2", "3"},
		// "#ref": "ref+1", // #标识此字段不做任何处理，SQL：set ref=ref+1
	}
	tb1.UpdateOne(updateData)
	// tb1.UpdateMany() 与 InsertMany 类似

	upData := map[string]interface{}{
		"name": "test",
		"json": []string{"1", "2", "3"},
	}
	upWhere := orm.Where{
		"id__gt": 1,
	}
	// 自定义更新条件
	_, err = tb1.UpdateByWhere(upData, upWhere)
	if err != nil {
		fmt.Println(err)
	}

	// 调用 ToData 之后的条件是否保持，true：保持，false：不保持，orm 默认保持
	tb1.KeepQuery(true)

	// 是否启用 select for update，true：启用，fasle：不启用，默认不启用
	tb1.SelectForUpdate(true)

	// 排序字段，默认升序，- 降序
	tb1.Order("id", "-ref") // id asc, ref desc

	// tb1.Limit(n) limit n
	// tb1.OverLimit(n, m) limit n,m
	// tb1.Page(no, size) limit size*(no-1),size
	// tb1.Distinct(b)
	pageData, err := tb1.PageData(&data, false, 1, 10)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(pageData)

	// 事务demo
	// 返回nil事务执行成功
	err = orm.TransSession(ctx, db, func(ctx context.Context, tx *orm.Tx) error {
		sessTb1 := orm.NewORMWithTx(ctx, "table1", tx, mysqlRef)
		_, err = sessTb1.InsertOne(map[string]interface{}{
			"name": "test",
		})
		if err != nil {
			return err
		}
		_, err = sessTb1.DeleteByWhere(orm.Where{
			"id__gt": 100,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
