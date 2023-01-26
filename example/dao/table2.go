package dao

type Table2 struct {
	ID   int32  `json:"id"`
	Name string `json:"name"`
	Age  int32  `json:"age"`
	Ref  int32  `json:"ref"`
	// tag字段
	Tb3 *Table3 `json:"tb3" ref:"left;ref=id"`
}
