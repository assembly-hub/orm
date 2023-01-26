# orm

mysql、mongo orm工具，提供丰富的CURD的功能，将sql的研发编写转为orm语法结构的编写，大大提高研发效率，降低业务复杂度

## 算子语法
### mysql
#### where、order、group、having等语法
```
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
```
#### select语法
```
Select 参数：* 主表所有字段；tag.* tag对应表所有字段；tag1.tag2.* tag1表的tag2的所有字段；以此类推
*0 等价 * 只考虑主表，不展开子表
*1 对应层级的表展开一层（主表+一级关联表，二级以下联表不算）
*2 对应层级的表展开二层（主表+一级关联表+二级关联表，三级以下联表不算）
*n 对应层级的表展开n层（主表+1级关联表+2级关联表+...+n级关联表，n+1级以下联表不算）
上述法则同样适用于tag.*1，此时的主表为tag，以此类推 tag1.tag2.*1，主表为tag1.tag2
字段排除：优先级最高
-name：删除name字段；-* 移除主表字段; -tag.name:删除tag表对应的name字段；-tag.*：删除整个tag表所有字段
排除字段也支持 *n 语法，-*n
*n与- 均不可与 # 混用
```