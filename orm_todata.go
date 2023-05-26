package orm

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/assembly-hub/db"
)

func scanMapList(rows db.Rows, flat bool, colLinkStr string, cacheLen int) (result []map[string]interface{}, err error) {
	if rows != nil {
		defer func(rows db.Rows) {
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

func toListData(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, flat bool, dataValue *reflect.Value) error {
	elemType := dataValue.Type().Elem()

	elemPtr := false
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
		elemPtr = true
	}

	switch elemType.Kind() {
	case reflect.Map:
		ret, err := toListDataMap(ctx, sqlDB, q, flat, elemType, elemPtr)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	case reflect.Struct:
		structData := q.RefConf.getTableCacheByTp(elemType)
		if structData == nil {
			ptr, err := computeStructData(elemType)
			if err != nil {
				return err
			}
			structData = ptr
		}
		ret, err := toListStruct(ctx, sqlDB, q, flat, structData, elemPtr)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	default:
		ret, err := toListSingleData(ctx, sqlDB, q, elemType)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	}

	return nil
}

func toData(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, result interface{}, flat bool) error {
	dataValue := reflect.ValueOf(result)
	if nil == result || dataValue.IsNil() || dataValue.Type().Kind() != reflect.Ptr {
		return ErrTargetNotSettable
	}

	if ctx == nil {
		ctx = context.Background()
	}

	dataValue = dataValue.Elem()
	tp := dataValue.Type()
	switch tp.Kind() {
	case reflect.Slice:
		return toListData(ctx, sqlDB, q, flat, &dataValue)
	case reflect.Map:
		ret, err := toFirstDataMap(ctx, sqlDB, q, flat, tp)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	case reflect.Struct:
		structData := q.RefConf.getTableCacheByTp(tp)
		if structData == nil {
			ptr, err := computeStructData(tp)
			if err != nil {
				return err
			}
			structData = ptr
		}
		ret, err := toFirstStruct(ctx, sqlDB, q, flat, structData)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	default:
		ret, err := toFirstSingleData(ctx, sqlDB, q, tp)
		if err != nil {
			return err
		}

		if ret != nil {
			dataValue.Set(*ret)
		}
	}

	return nil
}

func toFirstDataMap(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, flat bool,
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

	var rows db.Rows
	var err error
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.SQL())
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

func toListDataMap(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, flat bool,
	elemType reflect.Type, elemPtr bool) (*reflect.Value, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	kp := elemType.Key()
	if kp.Kind() != reflect.String {
		return nil, ErrMapKeyType
	}

	var rows db.Rows
	var err error
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.SQL())
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

func scanDataMapList(rows db.Rows, flat bool, colLinkStr string,
	cacheLen int, elemType reflect.Type, elemPtr bool) (result *reflect.Value, err error) {
	if rows != nil {
		defer func(rows db.Rows) {
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

func toListStruct(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, flat bool,
	structData *tableStructData, elemPtr bool) (*reflect.Value, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows db.Rows
	var err error
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.SQL())
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

func toFirstStruct(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, flat bool,
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

	var rows db.Rows
	var err error
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.SQL())
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

func scanDataStructList(rows db.Rows, flat bool, q *BaseQuery,
	cacheLen int, structData *tableStructData, elemPtr bool) (result *reflect.Value, err error) {
	if rows != nil {
		defer func(rows db.Rows) {
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

func toFirstSingleData(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, tp reflect.Type) (ret *reflect.Value, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if len(q.Limit) == 2 {
		q.Limit[1] = 1
	} else {
		q.Limit = []uint{1}
	}

	var rows db.Rows
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.SQL())
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

func toListSingleData(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery, tp reflect.Type) (ret *reflect.Value, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rows db.Rows
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.SQL())
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

func scanSingleList(rows db.Rows, cacheLen int, tp reflect.Type) (result *reflect.Value, err error) {
	if rows != nil {
		defer func(rows db.Rows) {
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
