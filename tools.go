package orm

import (
	"context"
	"fmt"
	"reflect"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
	"github.com/assembly-hub/db"
)

func TransSession(ctx context.Context, sqlDB db.Executor, f func(ctx context.Context, tx db.Tx) error) (err error) {
	tx, errTx := sqlDB.BeginTx(ctx, nil)
	if errTx != nil {
		return errTx
	}
	defer func() {
		if p := recover(); p != nil {
			err1 := tx.Rollback()
			err = fmt.Errorf("%v, Rollback=%w", p, err1)
		}
	}()

	err = f(ctx, tx)
	if err != nil {
		panic(err)
	}

	return tx.Commit()
}

func Struct2Map(raw interface{}, excludeKey ...string) map[string]interface{} {
	dataValue := reflect.ValueOf(raw)
	if dataValue.Type().Kind() != reflect.Struct && dataValue.Type().Kind() != reflect.Ptr {
		panic("data type must be struct or struct ptr")
	}

	if dataValue.Type().Kind() == reflect.Ptr {
		dataValue = dataValue.Elem()
	}

	if dataValue.Type().Kind() != reflect.Struct {
		panic("data type must be struct or struct ptr")
	}

	s := set.Set[string]{}
	s.Add(excludeKey...)

	m := map[string]interface{}{}
	for i := 0; i < dataValue.NumField(); i++ {
		colName := dataValue.Type().Field(i).Tag.Get("json")
		ref := dataValue.Type().Field(i).Tag.Get("ref")
		tp := dataValue.Type().Field(i).Tag.Get("type")
		if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
			continue
		}

		if !s.Has(colName) {
			if tp == "json" {
				m[colName] = util.Any2String(dataValue.Field(i).Interface())
			} else {
				m[colName] = dataValue.Field(i).Interface()
			}
		}
	}
	return m
}
