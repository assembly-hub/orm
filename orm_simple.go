// package orm
package orm

import (
	"context"
	"fmt"
	"time"
)

func GetMySQL() *DB {
	cfg := Config{}
	db, err := NewClient(&cfg).Connect()
	if err != nil {
		return nil
	}
	return db
}

type Tb1 struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Ref  int64  `json:"ref"`
	JSON []int  `json:"json" type:"json"`
	Tb   *Tb2   `json:"tb" ref:"left;ref=id"`
}

type Tb2 struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Tb   *Tb3   `json:"tb" ref:"left;tb=id"`
}

type Tb3 struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Tb   *Tb1   `json:"tb" ref:"left;tb=id"`
}

func SimpleInsertOne() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	one, err := q.InsertOne(map[string]interface{}{
		"id":      123,
		"1223":    "123",
		"dt":      time.Now(),
		"success": true,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(one)
}

func SimpleInsertOne2() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	d := Tb1{
		ID:   10,
		Name: "ceshi",
		Ref:  0,
	}
	one, err := q.InsertOne(&d)
	if err != nil {
		panic(err)
	}

	fmt.Println(one)
}

func SimpleInsertMany() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	d := Tb1{
		ID:   10,
		Name: "ceshi",
		Ref:  0,
	}
	one, _, err := q.InsertMany([]interface{}{
		d,
		Tb1{
			ID:   11,
			Name: "ceshi",
			Ref:  0,
		},
		map[string]interface{}{
			"name": "1111111111",
			"ref":  0,
		},
	}, true)
	if err != nil {
		panic(err)
	}

	fmt.Println(one)
}

func SimpleUpdateByWhere() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	one, err := q.UpdateByWhere(map[string]interface{}{
		"name": "123",
	}, map[string]interface{}{
		"id": 1,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(one)
}

func SimpleUpdateMany() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	many, err := q.UpdateMany([]interface{}{
		map[string]interface{}{
			"name": "123",
			"id":   1,
		},
	}, true)
	if err != nil {
		panic(err)
	}

	fmt.Println(many)
}

func SimpleReplaceOne() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	many, err := q.ReplaceOne(map[string]interface{}{
		"name": "123111",
		"id":   1,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(many)
}

func SimpleReplaceMany() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	_, ids, err := q.ReplaceMany([]interface{}{
		map[string]interface{}{
			"name": "123",
			"id":   1,
		},
	}, true)
	if err != nil {
		panic(err)
	}

	fmt.Println(ids)
}

func SimpleUpsertOne() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	id, err := q.UpsertOne(
		map[string]interface{}{
			"name": "123",
			"id":   1,
		})
	if err != nil {
		panic(err)
	}

	fmt.Println(id)
}

func SimpleDeleteByWhere() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	count, err := q.DeleteByWhere(map[string]interface{}{
		"id__in": []int{15, 14, 13},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
}

func SimpleCount() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	count, err := q.Count(false)
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
}

func SimpleToData() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	var v map[string]interface{}
	err := q.ToData(&v, false)
	if err != nil {
		panic(err)
	}

	fmt.Println(v)
}

func SimpleToData2() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	var v []Tb1
	err := q.ToData(&v, false)
	if err != nil {
		panic(err)
	}

	fmt.Println(v)
}

func TestTransSession() {
	mysqlCli := GetMySQL()
	err := TransSession(context.Background(), mysqlCli, func(ctx context.Context, tx *Tx) error {
		q := NewORMWithTx(ctx, "table1", tx, nil)
		_, err := q.UpdateOne(map[string]interface{}{
			"id":  28,
			"txt": "111",
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

func SimpleInsertManySameClos() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	rows, err := q.InsertManySameClos(
		[]interface{}{
			map[string]interface{}{
				"name": "t123",
				"z":    1,
			},
			map[string]interface{}{
				"name": "t1",
				"z":    "1",
			},
		}, []string{"name", "z"}, 100, false)

	if err != nil {
		panic(err)
	}

	fmt.Println(rows)
}

func SimpleReplaceManySameClos() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	rows, err := q.ReplaceManySameClos(
		[]interface{}{
			map[string]interface{}{
				"name": "t123",
				"z":    1,
			},
			map[string]interface{}{
				"name": "t1",
				"z":    "1",
			},
		}, []string{"name", "z"}, 100, false)

	if err != nil {
		panic(err)
	}

	fmt.Println(rows)
}

func SimpleUpsertManySameClos() {
	mysqlCli := GetMySQL()
	q := NewORM(context.Background(), "table1", mysqlCli, nil)
	rows, err := q.UpsertManySameClos(
		[]interface{}{
			map[string]interface{}{
				"name": "t123",
				"z":    1,
			},
			map[string]interface{}{
				"name": "t1",
				"z":    "1",
			},
		}, []string{"name", "z"}, 100, false)

	if err != nil {
		panic(err)
	}

	fmt.Println(rows)
}
