package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func fetchData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, dataType interface{}, flat bool,
	fetch func(interface{}) bool) error {
	dtType := reflect.TypeOf(dataType)
	if dtType.Kind() == reflect.Ptr {
		return ErrFetchType
	}

	if ctx == nil {
		ctx = context.Background()
	}

	switch dtType.Kind() {
	case reflect.Map:
		return toFetchDataMap(ctx, db, tx, q, flat, dtType, fetch)
	case reflect.Struct:
		structData := q.RefConf.GetTableCacheByTp(dtType)
		if structData == nil {
			ptr, err := computeStructData(dtType)
			if err != nil {
				return err
			}
			structData = ptr
		}
		return toFetchStruct(ctx, db, tx, q, flat, structData, fetch)
	default:
		return toFetchSingleData(ctx, db, tx, q, dtType, fetch)
	}
}

func toFetchDataMap(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool,
	dtType reflect.Type, fetch func(interface{}) bool) error {
	if ctx == nil {
		ctx = context.Background()
	}

	kp := dtType.Key()
	if kp.Kind() != reflect.String {
		return ErrMapKeyType
	}

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return ErrClient
	}

	if err != nil {
		return err
	}

	return scanFetchDataMap(rows, flat, q.SelectColLinkStr, dtType, fetch)
}

func scanFetchDataMap(rows *sql.Rows, flat bool, colLinkStr string,
	elemType reflect.Type, fetch func(interface{}) bool) (err error) {
	if rows != nil {
		defer func(rows *sql.Rows) {
			closeErr := rows.Close()
			if err != nil {
				err = fmt.Errorf("%w %v", err, closeErr)
			} else {
				err = closeErr
			}
		}(rows)
	} else {
		return nil
	}

	cols, err := rows.Columns()
	if err != nil {

		return err
	}

	if len(cols) == 0 {
		return ErrTooFewColumn
	}

	colType, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	vp := elemType.Elem()

	useDBType := vp.Kind() == reflect.Interface
	if !useDBType && !flat {
		return ErrParams
	}

	keyRow := make([]reflect.Value, len(cols))
	for i := range cols {
		keyRow[i] = reflect.ValueOf(cols[i])
	}

	valRow := make([]interface{}, len(cols))
	if useDBType {
		prepareValues(valRow, colType, cols)
	} else {
		for i := range cols {
			valRow[i] = reflect.New(reflect.PtrTo(vp)).Interface()
		}
	}

	for {
		if !rows.Next() {
			break
		}

		err = rows.Scan(valRow...)
		if err != nil {
			return err
		}

		mapVal := reflect.MakeMapWithSize(elemType, len(cols))
		scanIntoReflectMap(&mapVal, valRow, keyRow)
		if !flat && mapVal.Len() > 0 {
			m := mapVal.Interface().(map[string]interface{})
			formatMap(m, colLinkStr)
		}
		if !fetch(mapVal.Interface()) {
			break
		}
	}

	return nil
}

func toFetchStruct(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool,
	structData *tableStructData, fetch func(interface{}) bool) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return ErrClient
	}

	if err != nil {
		return err
	}

	return scanFetchDataStruct(rows, flat, q, structData, fetch)
}

func scanFetchDataStruct(rows *sql.Rows, flat bool, q *BaseQuery,
	structData *tableStructData, fetch func(interface{}) bool) (err error) {
	if rows != nil {
		defer func(rows *sql.Rows) {
			closeErr := rows.Close()
			if err != nil {
				err = fmt.Errorf("%w %v", err, closeErr)
			} else {
				err = closeErr
			}
		}(rows)
	} else {
		return nil
	}

	cols, err := rows.Columns()
	if err != nil {

		return err
	}

	if len(cols) == 0 {
		return ErrTooFewColumn
	}

	customType := reflect.TypeOf([]byte{})

	valRow := make([]interface{}, len(cols))
	fieldList := make([]innerStructField, len(cols))
	realCol := make([]int, 0, len(cols))
	for i := range cols {
		if f, ok := structData.FieldMap[cols[i]]; ok {
			realCol = append(realCol, i)
			if f.Custom {
				valRow[i] = reflect.New(reflect.PtrTo(customType)).Interface()
				fieldList[i] = innerStructField{
					IndexList: []int{f.Index},
					Custom:    true,
					TypeList:  []reflect.Type{f.DataType},
				}
				continue
			}

			valRow[i] = reflect.New(reflect.PtrTo(f.DataType)).Interface()
			fieldList[i] = innerStructField{
				IndexList: []int{f.Index},
				TypeList:  []reflect.Type{f.DataType},
			}
		} else if !flat {
			colArr := strings.Split(cols[i], q.SelectColLinkStr)
			if len(colArr) <= 1 {
				valRow[i] = new(interface{})
				continue
			}

			f = structData.FieldMap[colArr[0]]
			tpList, idxList, isCustom, err := getSubStructData(colArr, &f, q)
			if err != nil {
				return err
			}

			fieldList[i] = innerStructField{
				IndexList: idxList,
				Custom:    isCustom,
				Ref:       true,
				TypeList:  tpList,
			}
			if isCustom {
				valRow[i] = reflect.New(reflect.PtrTo(customType)).Interface()
			} else {
				valRow[i] = reflect.New(reflect.PtrTo(tpList[len(tpList)-1])).Interface()
			}
			realCol = append(realCol, i)
		} else {
			valRow[i] = new(interface{})
		}
	}

	for {
		if !rows.Next() {
			break
		}

		err = rows.Scan(valRow...)
		if err != nil {

			return err
		}

		objVal := reflect.New(structData.StructType)
		structVal := objVal.Elem()
		scanIntoReflectStruct(&structVal, valRow, fieldList, realCol)

		if !fetch(structVal.Interface()) {
			break
		}
	}

	return nil
}

func toFetchSingleData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, tp reflect.Type,
	fetch func(interface{}) bool) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return ErrClient
	}

	if err != nil {
		return err
	}

	return scanFetchSingle(rows, tp, fetch)
}

func scanFetchSingle(rows *sql.Rows, tp reflect.Type, fetch func(interface{}) bool) (err error) {
	if rows != nil {
		defer func(rows *sql.Rows) {
			closeErr := rows.Close()
			if err != nil {
				err = fmt.Errorf("%w %v", err, closeErr)
			} else {
				err = closeErr
			}
		}(rows)
	} else {
		return nil
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return ErrTooFewColumn
	}
	if len(cols) > 1 {
		return ErrTooManyColumn
	}

	nullVal := reflect.Zero(tp).Interface()
	val := reflect.New(reflect.PtrTo(tp))
	for {
		if !rows.Next() {
			break
		}

		err = rows.Scan(val.Interface())
		if err != nil {
			return err
		}

		vv := nullVal
		if reflectValue := reflect.Indirect(reflect.Indirect(val)); reflectValue.IsValid() {
			vv = reflectValue.Interface()
		}

		if !fetch(vv) {
			break
		}
	}

	return nil
}
