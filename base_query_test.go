package orm

import (
	"fmt"
	"testing"
	"time"
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

func TestMySqlOrmQuery(t *testing.T) {
	t1 := "table1"
	t2 := "table2"
	query := &queryModel{
		MainTable: t1,
		Select: []*selectModel{{
			Table: t1,
			Cols:  []string{"is_finished", "cur_status"},
		}, {
			Table: t2,
			Cols:  []string{"end_time"},
		}},
		JoinList: []*joinModel{{
			Type:      naturalLeftJoinType,
			MainTable: t1,
			JoinTable: t2,
			On: [][2]string{{
				"task_info_id", "id",
			}},
		}},
		Limit: []uint{1},
		Where: map[string]interface{}{
			t1 + ".is_valid": 1,
			t1 + ".id":       "123",
		},
	}

	fmt.Println(query.SQL())
}

func TestMySqlOrmConf(t *testing.T) {
	ref := NewReference()
	ref.addReference(&referenceData{
		FromTable: "table1",
		Type:      leftJoinType,
		Tag:       "tag1",
		ToTable:   "table2",
		On: [][2]string{
			{"id", "name"},
		},
	}, &referenceData{
		FromTable: "table1",
		Type:      leftJoinType,
		Tag:       "tag2",
		ToTable:   "table3",
		On: [][2]string{
			{"sid", "sname"},
		},
	}, &referenceData{
		FromTable: "table2",
		Type:      leftJoinType,
		Tag:       "tag3",
		ToTable:   "table5",
		On: [][2]string{
			{"id", "name"},
		},
	})

	q := &BaseQuery{
		RefConf:   ref,
		TableName: "table1",
		Select: []string{
			"id", "tag1.col", "tag1.tag3.sum(col2) as s",
		},
		Order: []string{
			"tag1.tag3.col",
			"-tag1.col",
			"#qwe",
		},
		GroupBy: []string{
			"#col3",
		},
		Distinct: false,
		Where: map[string]interface{}{
			"is_valid":          1,
			"id__gt":            "123",
			"tag1.tag3.col__in": []string{"1", "2", "3"},
			"tm":                time.Now(),
			"$or": []map[string]interface{}{
				{
					"c": 1,
					"$or": []map[string]interface{}{
						{
							"a": 1,
						},
						{
							"a": 2,
						},
					},
				}, {
					"c": 2,
					"$and": []map[string]interface{}{
						{
							"a": 2,
						},
						{
							"a": 6,
						},
					},
				},
			},
		},
	}

	s := q.SQL()
	fmt.Println(s)

	fmt.Println(q.GetWhere())
}

func TestMySqlOrmConf2(t *testing.T) {
	ref := NewReference()
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
			"tb2.tb3.name": "test",
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
	ref := NewReference()
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

func TestMySqlOrmConf31(t *testing.T) {
	ref := NewReference()
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
