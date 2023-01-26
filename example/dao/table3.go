package dao

type Table3 struct {
	ID   int32  `json:"id"`
	Name string `json:"name"`
	Info string `json:"info"`
	Ref  int32  `json:"ref"`
	// tag字段
	Tb1 *Table1 `json:"tb1" ref:"left;ref=id"`
}
