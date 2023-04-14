// Package orm
package orm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/assembly-hub/basics/util"
)

const (
	defCacheSize = 50
)

func time2Str(t interface{}) string {
	switch t := t.(type) {
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	case *time.Time:
		return t.Format("2006-01-02 15:04:05")
	}
	panic("parameter's type must be time.Time")
}

func str2Time(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return t
}

func prepareValues(values []interface{}, columnTypes []*sql.ColumnType, columns []string) {
	if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			values[idx] = newDataByDBType(columnType)
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func newDataByDBType(columnType *sql.ColumnType) interface{} {
	if columnType != nil && columnType.ScanType() != nil {
		return reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
	}
	return new(interface{})
}

func scanIntoReflectMap(mapValue *reflect.Value, values []interface{}, columns []reflect.Value) {
	for idx := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			if b, ok := reflectValue.Interface().(sql.RawBytes); ok {
				mapValue.SetMapIndex(columns[idx], reflect.ValueOf(string(b)))
				continue
			} else if valuer, ok := reflectValue.Interface().(driver.Valuer); ok {
				val, _ := valuer.Value()
				mapValue.SetMapIndex(columns[idx], reflect.ValueOf(val))
				continue
			}
			mapValue.SetMapIndex(columns[idx], reflectValue)
		} else {
			// reflectValue.SetMapIndex(columns[idx], reflect.Zero(mapValue.Type().Elem()))
		}
	}
}

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			if valuer, ok := reflectValue.Interface().(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
				continue
			} else if b, ok := reflectValue.Interface().(sql.RawBytes); ok {
				mapValue[column] = string(b)
				continue
			}
			mapValue[column] = reflectValue.Interface()
		} else {
			mapValue[column] = nil
		}
	}
}

func formatMap(mapValue map[string]interface{}, selectColLinkStr string) {
	for k, v := range mapValue {
		arr := strings.Split(k, selectColLinkStr)
		if len(arr) >= 2 {
			tempMap := mapValue
			for i, subKey := range arr {
				if i == len(arr)-1 {
					tempMap[subKey] = v
					break
				}

				if _, ok := tempMap[subKey]; !ok {
					m := make(map[string]interface{})
					tempMap[subKey] = m
					tempMap = m
				} else {
					tempMap = tempMap[subKey].(map[string]interface{})
				}
			}
			delete(mapValue, k)
		}
	}
}

func scanMapList(rows *sql.Rows, flat bool, colLinkStr string, cacheLen int) (result []map[string]interface{}, err error) {
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
		return nil, nil
	}

	cols, err := rows.Columns()
	if err != nil {

		return nil, err
	}
	colType, err := rows.ColumnTypes()
	if err != nil {

		return nil, err
	}

	if cacheLen <= 0 {
		cacheLen = defCacheSize
	}

	result = make([]map[string]interface{}, 0, cacheLen)
	row := make([]interface{}, len(cols))
	prepareValues(row, colType, cols)
	for {
		b := rows.Next()
		if !b {
			break
		}

		err = rows.Scan(row...)
		if err != nil {

			return nil, err
		}

		m := make(map[string]interface{})
		scanIntoMap(m, row, cols)
		if len(m) > 0 && !flat {
			formatMap(m, colLinkStr)
		}
		result = append(result, m)
	}

	return result, nil
}

func toListData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, result interface{}, flat bool, dataValue *reflect.Value) error {
	elemType := dataValue.Type().Elem()

	elemPtr := false
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		elemPtr = true
	}

	switch elemType.Kind() {
	case reflect.Map:
		ret, err := toListDataMap(ctx, db, tx, q, flat, elemType, elemPtr)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	case reflect.Struct:
		structData := q.RefConf.GetTableCacheByTp(elemType)
		if structData == nil {
			ptr, err := computeStructData(elemType)
			if err != nil {
				return err
			}
			structData = ptr
		}
		ret, err := toListStruct(ctx, db, tx, q, flat, structData, elemPtr)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	default:
		ret, err := toListSingleData(ctx, db, tx, q, elemType)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	}

	return nil
}

func toData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, result interface{}, flat bool) error {
	dataValue := reflect.ValueOf(result)
	if nil == result || dataValue.IsNil() || dataValue.Type().Kind() != reflect.Ptr {
		return ErrTargetNotSettable
	}

	if ctx == nil {
		ctx = context.Background()
	}

	dataValue = dataValue.Elem()
	switch dataValue.Type().Kind() {
	case reflect.Slice:
		return toListData(ctx, db, tx, q, result, flat, &dataValue)
	case reflect.Map:
		ret, err := toFirstDataMap(ctx, db, tx, q, flat, dataValue.Type())
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	case reflect.Struct:
		structData := q.RefConf.GetTableCacheByTp(dataValue.Type())
		if structData == nil {
			ptr, err := computeStructData(dataValue.Type())
			if err != nil {
				return err
			}
			structData = ptr
		}
		ret, err := toFirstStruct(ctx, db, tx, q, flat, structData)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	default:
		ret, err := toFirstSingleData(ctx, db, tx, q, dataValue.Type())
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	}

	return nil
}

func toFirstDataMap(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool,
	elemType reflect.Type) (*reflect.Value, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	kp := elemType.Key()
	if kp.Kind() != reflect.String {
		return nil, ErrMapKeyType
	}

	q.Limit = []uint{1}
	if len(q.Limit) == 2 {
		q.Limit[1] = 1
	} else {
		q.Limit = []uint{1}
	}

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return nil, ErrClient
	}

	if err != nil {
		return nil, err
	}

	result, err := scanDataMapList(rows, flat, q.SelectColLinkStr, 1, elemType, false)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}

	elem := result.Index(0)
	return &elem, nil
}

func toListDataMap(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool,
	elemType reflect.Type, elemPtr bool) (*reflect.Value, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	kp := elemType.Key()
	if kp.Kind() != reflect.String {
		return nil, ErrMapKeyType
	}

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return nil, ErrClient
	}

	if err != nil {
		return nil, err
	}

	cacheLen := defCacheSize
	if len(q.Limit) == 1 {
		cacheLen = int(q.Limit[0])
	} else if len(q.Limit) == 2 {
		cacheLen = int(q.Limit[1])
	}

	result, err := scanDataMapList(rows, flat, q.SelectColLinkStr, cacheLen, elemType, elemPtr)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}

	return result, nil
}

func scanDataMapList(rows *sql.Rows, flat bool, colLinkStr string,
	cacheLen int, elemType reflect.Type, elemPtr bool) (result *reflect.Value, err error) {
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
		return nil, nil
	}

	cols, err := rows.Columns()
	if err != nil {

		return nil, err
	}

	if len(cols) == 0 {
		return nil, ErrTooFewColumn
	}

	colType, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	if cacheLen <= 0 {
		cacheLen = defCacheSize
	}

	vp := elemType.Elem()

	useDBType := vp.Kind() == reflect.Interface
	if !useDBType && !flat {
		return nil, ErrParams
	}

	sliceType := elemType
	if elemPtr {
		sliceType = reflect.PtrTo(sliceType)
	}

	elemList := reflect.MakeSlice(reflect.SliceOf(sliceType), 0, cacheLen)
	result = &elemList

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

			return nil, err
		}

		mapVal := reflect.MakeMapWithSize(elemType, len(cols))
		scanIntoReflectMap(&mapVal, valRow, keyRow)
		if !flat && mapVal.Len() > 0 {
			m := mapVal.Interface().(map[string]interface{})
			formatMap(m, colLinkStr)
		}

		if elemPtr {
			v := reflect.New(elemType)
			v.Elem().Set(mapVal)
			elemList = reflect.Append(elemList, v)
		} else {
			elemList = reflect.Append(elemList, mapVal)
		}
	}

	return result, nil
}

func toListStruct(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool,
	structData *tableStructData, elemPtr bool) (*reflect.Value, error) {
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
		return nil, ErrClient
	}

	if err != nil {
		return nil, err
	}

	cacheSize := defCacheSize
	if len(q.Limit) == 1 {
		cacheSize = int(q.Limit[0])
	} else if len(q.Limit) == 2 {
		cacheSize = int(q.Limit[1])
	}

	result, err := scanDataStructList(rows, flat, q, cacheSize, structData, elemPtr)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}
	return result, nil
}

func toFirstStruct(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool,
	structData *tableStructData) (*reflect.Value, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	q.Limit = []uint{1}
	if len(q.Limit) == 2 {
		q.Limit[1] = 1
	} else {
		q.Limit = []uint{1}
	}

	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return nil, ErrClient
	}

	if err != nil {
		return nil, err
	}

	result, err := scanDataStructList(rows, flat, q, 1, structData, false)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}

	elem := result.Index(0)
	return &elem, nil
}

func getSubStructData(refArr []string, structField *structField,
	q *BaseQuery) (colType []reflect.Type, fieldIdx []int, b bool, err error) {
	tempField := structField
	for i := range refArr {
		fieldIdx = append(fieldIdx, tempField.Index)
		if tempField.Custom {
			b = true
		}

		colType = append(colType, tempField.DataType)

		if tempField.Ref {
			tbStruct := q.RefConf.GetTableCacheByTp(tempField.DataType)
			if tbStruct == nil {
				tbStruct, err = computeStructData(tempField.DataType)
				if err != nil {
					return
				}
			}

			ref := refArr[i+1]
			f := tbStruct.FieldMap[ref]
			tempField = &f
		}
	}

	return
}

type innerStructField struct {
	Custom    bool
	Ref       bool
	IndexList []int
	TypeList  []reflect.Type
}

func scanDataStructList(rows *sql.Rows, flat bool, q *BaseQuery,
	cacheLen int, structData *tableStructData, elemPtr bool) (result *reflect.Value, err error) {
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
		return nil, nil
	}

	cols, err := rows.Columns()
	if err != nil {

		return nil, err
	}

	if len(cols) == 0 {
		return nil, ErrTooFewColumn
	}

	if cacheLen <= 0 {
		cacheLen = defCacheSize
	}

	sliceTy := structData.StructType
	if elemPtr {
		sliceTy = reflect.PtrTo(sliceTy)
	}

	elemList := reflect.MakeSlice(reflect.SliceOf(sliceTy), 0, cacheLen)
	result = &elemList

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
				return nil, err
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

			return nil, err
		}

		objVal := reflect.New(structData.StructType)
		structVal := objVal.Elem()
		scanIntoReflectStruct(&structVal, valRow, fieldList, realCol)

		if elemPtr {
			elemList = reflect.Append(elemList, objVal)
		} else {
			elemList = reflect.Append(elemList, structVal)
		}

	}

	return result, nil
}

func scanIntoReflectStruct(obj *reflect.Value, row []interface{}, fieldList []innerStructField, realCol []int) {
	for _, idx := range realCol {
		val := row[idx]
		field := fieldList[idx]

		if reflectVal := reflect.Indirect(reflect.Indirect(reflect.ValueOf(val))); reflectVal.IsValid() {
			if field.Ref {
				temp := obj
				for i := 0; i < len(field.IndexList)-1; i++ {
					fid := field.IndexList[i]
					fv := temp.Field(fid)
					tp := fv.Type()

					isPtr := false
					if tp.Kind() == reflect.Ptr {
						isPtr = true
						fv = fv.Elem()
						tp = tp.Elem()
					}

					if fv.IsValid() {
						temp = &fv
					} else {
						structVal := reflect.New(tp)
						if isPtr {
							temp.Field(fid).Set(structVal)
							structVal = structVal.Elem()
						} else {
							structVal = structVal.Elem()
							fv.Set(structVal)
						}
						temp = &structVal
					}
				}

				dataInStructField(temp, reflectVal, field.IndexList[len(field.IndexList)-1],
					field.TypeList[len(field.IndexList)-1], field.Custom)
			} else {
				dataInStructField(obj, reflectVal, field.IndexList[0], field.TypeList[0], field.Custom)
			}
		}
	}
}

func dataInStructField(obj *reflect.Value, reflectVal reflect.Value, fieldIndex int, dataType reflect.Type, custom bool) {
	if custom {
		bytes := reflectVal.Bytes()
		if len(bytes) <= 0 {
			return
		}

		newVal := reflect.New(dataType)
		err := json.Unmarshal(bytes, newVal.Interface())
		if err != nil {
			panic(err)
		}
		obj.Field(fieldIndex).Set(newVal.Elem())
	} else {
		obj.Field(fieldIndex).Set(reflectVal)
	}
}

func toFirstSingleData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, tp reflect.Type) (ret *reflect.Value, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if len(q.Limit) == 2 {
		q.Limit[1] = 1
	} else {
		q.Limit = []uint{1}
	}

	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return nil, ErrClient
	}

	if err != nil {
		return nil, err
	}

	result, err := scanSingleList(rows, 1, tp)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}

	elem := result.Index(0)
	return &elem, nil
}

func toListSingleData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, tp reflect.Type) (ret *reflect.Value, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.SQL())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.SQL())
	} else {
		return nil, ErrClient
	}

	if err != nil {
		return nil, err
	}

	cacheLen := defCacheSize
	if len(q.Limit) == 1 {
		cacheLen = int(q.Limit[0])
	} else if len(q.Limit) == 2 {
		cacheLen = int(q.Limit[1])
	}

	result, err := scanSingleList(rows, cacheLen, tp)
	if err != nil {
		return nil, err
	}

	if result.Len() <= 0 {
		return nil, nil
	}
	return result, nil
}

func scanSingleList(rows *sql.Rows, cacheLen int, tp reflect.Type) (result *reflect.Value, err error) {
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
		return nil, nil
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, ErrTooFewColumn
	}
	if len(cols) > 1 {
		return nil, ErrTooManyColumn
	}

	if cacheLen <= 0 {
		cacheLen = defCacheSize
	}

	elemList := reflect.MakeSlice(reflect.SliceOf(tp), 0, cacheLen)
	result = &elemList

	nullVal := reflect.Zero(tp)
	val := reflect.New(reflect.PtrTo(tp))
	for {
		if !rows.Next() {
			break
		}

		err = rows.Scan(val.Interface())
		if err != nil {

			return nil, err
		}

		if reflectValue := reflect.Indirect(reflect.Indirect(val)); reflectValue.IsValid() {
			elemList = reflect.Append(elemList, reflectValue)
		} else {
			elemList = reflect.Append(elemList, nullVal)
		}
	}

	return result, nil
}

func count(ctx context.Context, db *DB, tx *Tx, q *BaseQuery) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, q.Count())
	} else if db != nil {
		rows, err = db.QueryContext(ctx, q.Count())
	} else {
		return 0, ErrClient
	}
	if err != nil {
		return 0, err
	}

	result, err := scanMapList(rows, false, q.SelectColLinkStr, 1)
	if err != nil {
		return 0, err
	}

	if len(result) <= 0 {
		return 0, nil
	}

	return util.Str2Int[int64](fmt.Sprintf("%v", result[0]["c"]))
}

func connectStrArr(arr []string, linkStr string, start, end string) string {
	var s strings.Builder
	s.Grow(len(arr) * (len(linkStr) + len(start) + len(end) + 10))
	for _, v := range arr {
		if s.Len() > 0 {
			s.WriteString(linkStr)
		}

		s.WriteString(start)
		s.WriteString(v)
		s.WriteString(end)
	}

	return s.String()
}
