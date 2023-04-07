package orm

import (
	"fmt"
	"testing"

	"github.com/assembly-hub/orm/dbtype"
)

func TestBaseGetData(t *testing.T) {
	p := &queryModel{
		MainTable: "table1",
		Select: []*selectModel{
			{
				Cols: []string{"table1.col1", "count(id) as c"},
			},
		},
		Order: []*orderModel{
			{
				Cols: []string{"col2"},
			},
		},
		Where: map[string]interface{}{
			"test":            1,
			"`key`__eq":       "2",
			"key2__icontains": []string{"111", "222", "333"},
			"$or": map[string]interface{}{
				"test":      1,
				"`key`__eq": "2",
				"$and": map[string]interface{}{
					"test":      1,
					"`key`__eq": "2",
				},
			},
			"b__in": []int64{1, 2, 3},
			"c__in": []string{"1", "2", "3"},
		},
		GroupBy: []string{"id"},
		Having: map[string]interface{}{
			"c__gt":  1,
			"c__lte": 2,
		},
		Limit: []uint{1, 2},
		JoinList: []*joinModel{
			{
				Type:      "left",
				MainTable: "table1",
				JoinTable: "table2",
				On: [][2]string{
					{"c1", "c2"},
					{"c1", "c3"},
				},
			},
			{
				Type:      "inner",
				MainTable: "table1",
				JoinTable: "table3",
				On: [][2]string{
					{"c1", "c2"},
				},
			},
		},
	}

	s := p.SQL()
	fmt.Println(s)
}

func TestBaseGetData2(t *testing.T) {
	p1 := &queryModel{
		MainTable: "table1",
		Select: []*selectModel{
			{
				Table: "table1",
				Cols:  []string{"col1"},
			},
		},
		Order: []*orderModel{
			{
				Table: "table1",
				Cols:  []string{"col2"},
			},
		},
		Where: map[string]interface{}{
			"test":            1,
			"k__eq":           "2",
			"key2__icontains": []string{"111", "222", "333"},
			"$or": map[string]interface{}{
				"test":  1,
				"k__eq": "2",
				"$and": map[string]interface{}{
					"test":  1,
					"k__eq": "2",
				},
			},
		},
		JoinList: []*joinModel{
			{
				Type:      "left",
				MainTable: "table1",
				JoinTable: "table2",
				On: [][2]string{
					{"c1", "c2"},
					{"c1", "c3"},
				},
			},
			{
				Type:      "inner",
				MainTable: "table1",
				JoinTable: "table3",
				On: [][2]string{
					{"c1", "c2"},
				},
			},
		},
	}

	p := &queryModel{
		MainTable: "table1",
		Select: []*selectModel{
			{
				Cols: []string{"table1.col1", "count(id) as c"},
			},
		},
		Order: []*orderModel{
			{
				Cols: []string{"col2"},
			},
		},
		Where: map[string]interface{}{
			"test":            1,
			"key2__icontains": []string{"111", "222", "333"},
			"$and": []map[string]interface{}{
				{
					"test":      1,
					"`key`__eq": "1",
					"$or": []map[string]interface{}{
						{
							"test":      1,
							"`key`__eq": "1",
						}, {
							"test":      2,
							"`key`__eq": "2",
						},
					},
				}, {
					"test":      2,
					"`key`__eq": "2",
					"$or": []map[string]interface{}{
						{
							"test":      1,
							"`key`__eq": "1",
						}, {
							"test":      2,
							"`key`__eq": "2",
						},
					},
				},
			},
			"a__in": p1,
			"b__in": []int64{1, 2, 3},
			"c__in": []string{"1", "2", "3"},
		},
		GroupBy: []string{"id"},
		Having: map[string]interface{}{
			"c__gt":  1,
			"c__lte": 2,
		},
		Limit: []uint{1, 2},
		JoinList: []*joinModel{
			{
				Type:      "left",
				MainTable: "table1",
				JoinTable: "table2",
				On: [][2]string{
					{"c1", "c2"},
					{"c1", "c3"},
				},
			},
			{
				Type:      "inner",
				MainTable: "table1",
				JoinTable: "table3",
				On: [][2]string{
					{"c1", "c2"},
				},
			},
		},
	}

	s := p.SQL()
	fmt.Println(s)
}

func TestMySQLOrmConf2(t *testing.T) {
	ref := NewReference(dbtype.MySQL)
	ref.AddTableDef("table1", Table1{})
	ref.AddTableDef("table2", Table2{})
	ref.AddTableDef("table3", Table3{})
	ref.BuildRefs()

	q := &BaseQuery{
		RefConf: ref,
		// SelectColLinkStr: "_",
		// SelectRaw: true,
		TableName: "table1",
		Select: []string{
			"*2", "-tb2.*", "-tb2.tb3.tb1.id", //"tb2.tb3.*1", // "-*",
			"count(*) as c",
		},
		Where: map[string]interface{}{
			"tb2.tb3.name__istartswith": "test",
		},
		Distinct: false,
	}

	var s = q.SQL()
	fmt.Println(s)
}

type Def2 struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Name1111111 string `json:"name1"`
	Ref         *Def   `json:"ref" ref:"left;id=ref_id"`
}

type Def struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	RefID int    `json:"ref_id"`
	Ref   *Def   `json:"ref" ref:"left;id=ref_id"`
	Tb2   *Def2  `json:"tb2" ref:"left;id=id"`
}

func TestMySQLOrmConf3(t *testing.T) {
	ref := NewReference(dbtype.MySQL)
	ref.AddTableDef("table1", Def{})
	ref.AddTableDef("table2", Def2{})
	ref.BuildRefs()

	q := &BaseQuery{
		RefConf:   ref,
		TableName: "table1",
		Select:    []string{},
		//BaseQuery: BaseQuery{
		//	"ref.tb2.ref.tb2.id": 1,
		//},
		Order: []string{"tb2.id"},
	}

	s := q.Count()
	fmt.Println(s)
}

func TestMySQLOrmConf31(t *testing.T) {
	ref := NewReference(dbtype.MySQL)
	ref.AddTableDef("table1", Def{})
	ref.AddTableDef("table2", Def2{})
	ref.BuildRefs()

	q1 := &BaseQuery{
		RefConf:   ref,
		TableName: "table1",
		Select:    []string{},
		Where: Where{
			"ref.tb2.ref.tb2.id__in": []int{1, 2, 3},
		},
		Order: []string{"tb2.id"},
	}

	q := &BaseQuery{
		RefConf:   ref,
		TableName: "table1",
		Select:    []string{},
		Where: Where{
			"ref.tb2.ref.tb2.id__in": []int{1, 2, 3},
			"~id__in":                q1,
		},
		Order: []string{"tb2.id"},
	}

	s := q.Count()
	fmt.Println(s)
}
