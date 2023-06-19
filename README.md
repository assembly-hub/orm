# ORM使用说明
## 一、初始化
### 表定义
```go
// 根据数据库类型定义表关系对象
var ref = orm.NewReference(dbtype.MySQL)

// 定义表结构
type Table1 struct {
    ID   int32    `json:"id"`
    Name string   `json:"name"`
    // 自定义类型
    JSON []string `json:"json" type:"json"` // 数据库类型为 varchar
    Ref  int32    `json:"ref"` // 实际的关联table2的字段
    // tb2：可以认为是table2的别名，可以直接tb2.key,key是table2的字段名称
    // left：是left join；ref=id 关联方式，本表字段=关联表字段，多个用逗号隔开，如：ref=id,name=name
    Tb2 *Table2 `json:"tb2" ref:"left;ref=id"` // 含义：from table1 left join table2 on ref=id
}
// 添加表定义（建议放在表声明的go文件的 init 方法中）
ref.AddTableDef("table1", dao.Table1{})

type Table2 struct {
    ID   int32  `json:"id"`
    Name string `json:"name"`
    Age  int32  `json:"age"`
    Ref  int32  `json:"ref"`
    // tag字段
    Tb3 *Table3 `json:"tb3" ref:"left;ref=id"` // 含义：from table2 left join table3 on ref=id
}
ref.AddTableDef("table2", dao.Table2{})

type Table3 struct {
    ID   int32  `json:"id"`
    Name string `json:"name"`
    Info string `json:"info"`
    Ref  int32  `json:"ref"`
    // tag字段
    Tb1 *Table1 `json:"tb1" ref:"left;ref=id"` // 含义：from table3 left join table1 on ref=id
}
ref.AddTableDef("table3", dao.Table3{})

// 编译整个数据库的表关系
ref.BuildRefs()
```
> 其中原表对目标表的关联字段成为 tag 字段，跨表查询采用 tag 计算逻辑，如：\
> tag.id 实际会查询 tag 对应表的 id 字段

**注：以上仅仅在项目启动执行一次，切勿在业务代码中执行调用**

## 二、查询算子（每个算子前面需要用双下划线标注）
**以下所有算子均可在where、order、group、having中使用**
> tb1 := orm.NewORM(ctx, "table1", db, ref)
**查询有：Query Where Wheres方法，一下以 Where 为演示使用**

### 1、eq 等于（唯一一个可以省略的算子）
> tb1.Where("id__eq", 1) or tb1.Where("id", 1)

对应sql
```sql
select * from `table1` where id=1
```
### 2、ne 不等于
> tb1.Where("id__ne", 1)

对应sql
```sql
select * from `table1` where id<>1
```
### 3、lt 小于
> tb1.Where("id__lt", 1)

对应sql
```sql
select * from `table1` where id<1
```
### 4、lte 小于等于
> tb1.Where("id__lte", 1)

对应sql
```sql
select * from `table1` where id<=1
```
### 5、gt 大于
> tb1.Where("id__gt", 1)

对应sql
```sql
select * from `table1` where id>1
```
### 6、gte 大于等于
> tb1.Where("id__gte", 1)

对应sql
```sql
select * from `table1` where id>=1
```
### 7、in 包含
> tb1.Where("id__in", []int{1,2,3})

> tb1.Where("id__in", []string{"1","2","3"})

对应sql
```sql
select * from `table1` where id in (1,2,3)
select * from `table1` where id in ('1','2','3')
```
### 8、nin 不包含
> tb1.Where("id__nin", []int{1,2,3})

> tb1.Where("id__nin", []string{"1","2","3"})

对应sql
```sql
select * from `table1` where id not in (1,2,3)
select * from `table1` where id not in ('1','2','3')
```
### 9、date 日期
> tb1.Where("dt__date", "2023-01-01")

对应sql
```sql
select * from `table1` where dt>='2023-01-01' and dt<'2023-01-02'
```
### 10、between 范围
> tb1.Where("dt__between", []int{1,2})

对应sql
```sql
select * from `table1` where dt between 1 and 2
```
***以上均支持子查询，也就是参数可以是orm对象***

---

### 11、null 判空
> tb1.Where("dt__null", true)

> tb1.Where("dt__null", false)

对应sql
```sql
select * from `table1` where dt is null
select * from `table1` where dt is not null
```

### 12、startswith 匹配开始
> tb1.Where("str__startswith", "start")

对应sql
```sql
select * from `table1` where str like 'start%'
```
### 13、endswith 匹配结束
> tb1.Where("str__endswith", "end")

对应sql
```sql
select * from `table1` where str like '%end'
```
### 14、contains 匹配包含
> tb1.Where("str__contains", "str")

对应sql
```sql
select * from `table1` where str like '%str%'
```
> tb1.Where("str__contains", []string{"s1", "s2"})

对应sql
```sql
select * from `table1` where (str like '%s1%' and str like '%s2%')
```
### 15、customlike 自定义like
> tb1.Where("str__customlike", "__d")

对应sql
```sql
select * from `table1` where str like '__d'
```
> tb1.Where("str__customlike", []string{"_nd%", "_art"})

对应sql
```sql
select * from `table1` where (str like '_nd%' and str like '_art')
```
### 16、orstartswith 或匹配开始
> tb1.Where("str__orstartswith", "d")

对应sql
```sql
select * from `table1` where str like 'd%'
```
> tb1.Where("str__orstartswith", []string{"d", "dd"})

对应sql
```sql
select * from `table1` where (str like 'd%' or str like 'dd%')
```
### 17、orendswith 或匹配结束
> tb1.Where("str__orendswith", "d")

对应sql
```sql
select * from `table1` where str like '%d'
```
> tb1.Where("str__orendswith", []string{"d", "dd"})

对应sql
```sql
select * from `table1` where (str like '%d' or str like '%dd')
```
### 18、orcontains 或匹配包含
> tb1.Where("str__orcontains", "d")

对应sql
```sql
select * from `table1` where str like '%d%'
```
> tb1.Where("str__orcontains", []string{"d", "dd"})

对应sql
```sql
select * from `table1` where (str like '%d%' or str like '%dd%')
```
### 19、orcustomlike 或匹配like
> tb1.Where("str__orcustomlike", "__d")

对应sql
```sql
select * from `table1` where str like '__d'
```
> tb1.Where("str__orcustomlike", []string{"_d%", "%d_"})

对应sql
```sql
select * from `table1` where (str like '_d%' or str like '%d_')
```
***以上为基础***

---

***注：$or $and内部即可以包含基础，也可以嵌套 $or $and ***
### 20、$or 或，其内部为基础
```go
tb1.Where("$or", map[string]interface{}{
    "id__gt": 0,
    "name__startswith": "str",
})
```

对应sql
```sql
select * from `table1` where id>0 or name like "str%"
```
```go
tb1.Where("$or", []map[string]interface{}{
    {
        "id__gt": 0,
        "name__startswith": "str",
    },{
        "id__lt": 0,
        "name": "string",
    }
})
```

对应sql
```sql
select * from `table1` where (id>0 and name like 'str%') or (id<0 and name='string')
```

### 21、$and 与，其内部为基础
```go
tb1.Where("$and", map[string]interface{}{
    "id__gt": 0,
    "name__startswith": "str",
})
```

对应sql
```sql
select * from `table1` where id>0 and name like "str%"
```
```go
tb1.Where("$and", []map[string]interface{}{
    {
        "id__gt": 0,
        "name__startswith": "str",
    },{
        "id__lt": 0,
        "name": "string",
    }
})
```

对应sql
```sql
select * from `table1` where (id>0 and name like 'str%') and (id<0 and name='string')
```

### 22、tag 操作符
> tag 需要在表定义中指定，之后可以随时使用

> 例子：定义两个表，其中 Table1 关联 Table2
```go
type Table1 struct {
    ...
    Tb2 *Table2 `json:"tb2" ref:"left;ref=id"` // 含义：from table1 left join table2 on ref=id
}

type Table2 struct {
    ID   int32  `json:"id"`
    Name string `json:"name"`
}
```
> tag 使用例子

> tb1.Wheres("tb2.id__gt", 1)

>tb1.Wheres("tb2.name__startswith", "test")

对应sql
```sql
select * from `table1` left join `table2` on `table1`.ref=`table2`.id
where `table2`.id>1 and `table2`.name like 'test%'
```

### 23、数据库函数使用，以 count 为例
> tb1.Wheres("tb2.count(id)__gt", 1)

对应sql
```sql
select * from `table1` left join `table2` on `table1`.ref=`table2`.id
where count(`table2`.id)>1
```

### 24、# 原始字段限定符
> 表明被 # 修饰的字段不进行 tag 规则计算，不进行格式化的计算等操作

> tb1.Wheres("#count(tb2_id)__gt", 1)

对应sql
```sql
select * from `table1` left join `table2` on `table1`.ref=`table2`.id
where count(tb2_id)>1
```

### 25、 ~ 取反操作符
> ~ 为条件取反，必须在最前面，可用在所有前面，如果与#连用，#应在~后面，如：~#test

> tb1.Where("~id__lt", 1)

对应sql
```sql
select * from `table1` where not (id<1)
```

## 三、select 查询
### 1、*n 语法
```
Select 参数：* 主表所有字段；tag.* tag对应表所有字段；tag1.tag2.* tag1表的tag2的所有字段；以此类推
    *0 等价 * 只考虑主表，不展开子表
    *1 对应层级的表展开一层（主表+一级关联表，二级以下联表不算）
    *2 对应层级的表展开二层（主表+一级关联表+二级关联表，三级以下联表不算）
    *n 对应层级的表展开n层（主表+1级关联表+2级关联表+...+n级关联表，n+1级以下联表不算）
上述法则同样适用于tag.*1，此时的主表为tag，以此类推 tag1.tag2.*1，主表为tag1.tag2
```
```
- : 字段排除,优先级最高
-name：删除name字段；-* 移除主表字段; -tag.name:删除tag表对应的name字段；-tag.*：删除整个tag表所有字段
排除字段也支持 *n 语法，-*n
```
> !!! *n与- 均不可与 # 混用
---
> tb1.Select("*2", "-tb2.name") \
> 其中: *2 字段包含 id,name,json,ref,tb2.id,tb2.name,tb2.age,tb2.ref \
> -tb2.name: 去除 tb2.name \
> 实际的字段为：id,name,json,ref,tb2.id,tb2.age,tb2.ref

### 2、数据库函数使用
> tb1.Select("sum(id)", "tb2.sum(id)")

对应sql
```sql
select sum(id) as `id_sum`, sum(`orm_tb2`.`id`) as tb2_id_count
```
> sum(`orm_tb2`.`id`) as tb2_id_sum\
> 其中 orm 为框架固定前缀 \
> "_" 为链接符，默认为 "_", 可以自定义 tb1.SelectColLinkStr("自定义字符串") 除非很有不要，否则不建议修改

### 3、自定义字段别名，别名可以结合 # 直接在 where、group、order 中使用
> tb1.Select("sum(id) as s", "tb2.sum(id) as s2") \
> tb1.Where("#s__gt", 1)

对应sql
```sql
select sum(id) as s, sum(`orm_tb2`.`id`) as s2
...
where s>1
```

### 4、# 符合使用，自定义字段，不会进行编译
> tb1.Select("#sum(id)")

对应sql
```sql
select sum(id)
```

## 四、order by，支持 # 与 tag 操作
### 1、简单排序
> tb1.Order("id","-name")

对应sql
```sql
order by id asc,name desc
```
### 2、tag 字段排序
> tb1.Order("tb2.id","-tb2.name")

对应sql
```sql
order by `table2`.id asc,`table2`.`name` desc
```
### 3、# 字段排序
> tb1.Select("sum(id) as s") \
> tb1.Order("#s","-tb2.name")

对应sql
```sql
order by s asc,`table2`.`name` desc
```

## 五、group by 与 order by 类似
## 六、having，必须与 group by 结合使用

## 七、ORM 函数介绍
### 1、SetDefLimit(n uint) 设置默认limit
> 默认select all，为避免数据量太大，可以设置默认 limit，当检查没有设置limit时，将使用默认limit \
> 当 n > 0，启用默认limit；当 n == 0，关闭默认limit，默认为关闭
### 2、SQLServerExcludePK
> 针对SQL Server的定制，sql server主键采用自增时，禁止主动插入，可以通过此方法配置是否排除主键
### 3、OracleMergeUnionAll
> 针对oracle的定制，oracle在merge into时需要联合数据，此方法配置数据的链接方式，默认为union all，可以通过配置配置为 union
### 4、UniqueKeys
> 配置用于Upsert的唯一键
### 5、SelectColLinkStr
> 设置别名链接字符串，不建议修改 \
> 其中当数据接收参数 flat=false，其默认值是"__"；flat=true，其默认值"_"
### 6、CustomSQL(sql string)
> 用户自定义查询sql语句
### 7、Query Where Wheres
> 查询条件函数
### 8、OverLimit(over, size uint)
> 配置 limit 和 offset
### 9、Page(pageNo, pageSize uint)
> 传入页码和每页的大小，框架自定转化为对应数据库的数据查询条件
### 10、Limit(size uint)
> 设置limit
### 11、Distinct
> 设置是否去重
### 12、SelectForUpdate
> 设置 select for update, 注意：需要数据库支持
### 13、Select 配置字段查询
### 14、Order 配置排序字段
### 15、GroupBy 配置分组字段
### 16、Having HavingSome 类似 Where Wheres 用于分组之后的查询条件设置
### 17、ToData(result interface{}, flat bool) 万能数据接收接口
```go
其中 result 为数据指针，数据类型如下：
    1、简单类型：int、string、uint等
    2、简单切片：[]int []string []uint等
    3、map类型：map[string]interface{}、map[string]int、map[string]string等
    4、map切片：[]map[string]interface{}、[]map[string]int、[]map[string]string等
    5、struct：Table1
    6、struct切片：[]Table1

flat参数指定返回的数据是否按照表之间的关联关系嵌套展示，
false：需要嵌套（仅支持 map[string]interface{}、[]map[string]interface{}、struct、[]struct类型），其他类型必须是true
true：打平展示，无需嵌套
```
> demo
```go
var res []Table1
tb1.ToDate(&res, false)
```
flat: false 数据嵌套，分层展示
```go
{
    "key1": "test",
    "key2": "test",
    "key3": "test",
    "key4": {
        "k1": 1
    }
}
```
flat: true 数据打平，一层展示（对应的key为每一层的key用"_"连起来，如：key4_k1）
```go
{
    "key1": "test",
    "key2": "test",
    "key3": "test",
    "key4_k1": 1
}
```

### 18、FetchData(dataType interface{}, flat bool, fetch func(row interface{}) bool) 万能数据接收接口，用于未知数据量或者大数据量
> dataType 指定数据类型（传入对应数据类型的任意值）

> flat 同ToData

> fetch 数据推送方法，需要自定义；返回true，继续接收下一行数据，返回false，停止数据查询，其中 row 实际类型与 dataType 一样，可断言获取

实例如下：
```go
// 采用 string 接收数据
err := tb1.FetchData("", true, func(row interface{}) bool {
    if row != nil {
        fmt.Println(row.(string))
    }
    // 继续接收数据
    return true
})

// 采用 map[string]string 接收数据
err := tb1.FetchData(map[string]string{}, true, func(row interface{}) bool {
    if row != nil {
        fmt.Println(row.(map[string]string))
    }
    // 继续接收数据
    return true
})

// 采用 map[string]interface{} 接收数据
err := tb1.FetchData(map[string]interface{}{}, false, func(row interface{}) bool {
    if row != nil {
        fmt.Println(row.(map[string]interface{}))
    }
    // 继续接收数据
    return true
})

// 采用 Table1 接收数据
err := tb1.FetchData(Table1{}, true, func(row interface{}) bool {
    if row != nil {
        fmt.Println(row.(Table1))
    }
    // 继续接收数据
    return true
})
```

### 19、PageData(result interface{}, flat bool, pageNo, pageSize uint) (pg *Paging, err error) 获取某一页的数据
> 参数与ToData一致
```go
type Paging struct {
    PageNo    int `json:"page_no"`    //当前页
    PageSize  int `json:"page_size"`  //每页条数
    Total     int `json:"total"`      //总条数
    PageTotal int `json:"page_total"` //总页数
}
```
### 20、ExecuteSQL(customSQL string) (affectedRow int64, err error)
> 执行自定义sql，如：update、insert、delete、select等，返回受影响的行数
### 21、Exist
> 检查是否有数据
### 22、Count 获取数据条数
### 23、数据插入
#### 1、InsertOne
> 数据类型可以是 map[string]interface{} 或 struct
#### 2、InsertMany
> 参数可以是 map 与 struct 混合的数组，trans: true可以开启事务
#### 3、InsertManySameClos 大数据推荐此方法
> 参数可以是 map 与 struct 混合的数组，trans: true可以开启事务，cols 列字段, batchSize每个批次的大小
### 24、数据更新或插入
#### 1、UpsertOne
> 数据类型可以是 map[string]interface{} 或 struct
#### 2、UpsertMany
> 参数可以是 map 与 struct 混合的数组，trans: true可以开启事务
#### 3、UpsertManySameClos 大数据推荐此方法
> 参数可以是 map 与 struct 混合的数组，trans: true可以开启事务，cols 列字段, batchSize每个批次的大小

### 25、数据插入或保存 SaveMany，根据主键id是否存在，动态执行insert或update
### 26、UpdateMany 主键，id 不能为空，为空将更新失败
### 27、UpdateByWhere 根据条件进行数据批量的更新
### 28、UpdateOne 主键，id 不能为空，为空将更新失败
### 29、DeleteByWhere 根据条件删除数据
### 30、更新或替换，仅支持：MySQL MariaDB SQLite2\3，推荐使用Upsert系列方法
#### 1、ReplaceOne 与 UpsertOne类似
#### 2、ReplaceMany 与 UpsertMany类似
#### 3、ReplaceManySameClos 与 UpsertManySameClos类似

## 八、事务 orm.TransSession
```go
err = orm.TransSession(ctx, dbConn, func(ctx context.Context, tx db.Tx) error {
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
```

## 九、其他
### 1、orm.Struct2Map
可以根据要求将struct转成map，过滤ref，格式化json自定义数据

## 十、结语
有问题随时留言，vx：lm2586127191
