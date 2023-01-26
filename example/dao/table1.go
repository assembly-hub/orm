package dao

type Table1 struct {
	ID   int32    `json:"id"`
	Name string   `json:"name"`
	JSON []string `json:"json" type:"json"`
	Ref  int32    `json:"ref"` // 实际的关联table2的字段
	// tb2：可以认为是table2的别名，可以直接tb2.key,key是table2的字段名称
	// left：是left join；ref=id 关联方式，本表字段=关联表字段，多个用逗号隔开，如：ref=id,name=name
	Tb2 *Table2 `json:"tb2" ref:"left;ref=id"`
}
