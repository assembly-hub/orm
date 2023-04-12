// Package orm
package orm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
)

const (
	structTypeName  = "type"
	structTypeValue = "json"
)

type structJSONType int

const (
	jsonSlice = structJSONType(0)
	jsonMap   = structJSONType(1)
	jsonStr   = structJSONType(2)
	jsonOther = structJSONType(3)
)

const defCacheSize = 50

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
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue[column] = reflectValue.Interface()
			if valuer, ok := mapValue[column].(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
			} else if b, ok := mapValue[column].(sql.RawBytes); ok {
				mapValue[column] = string(b)
			}
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
					m := map[string]interface{}{}
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

func toListMap(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool) ([]map[string]interface{}, error) {
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

	if rows != nil {
		defer func(rows *sql.Rows) {
			err = rows.Close()
			if err != nil {
				log.Println(err.Error())
			}
		}(rows)
	}

	cacheLen := defCacheSize
	if len(q.Limit) == 1 {
		cacheLen = int(q.Limit[0])
	} else if len(q.Limit) == 2 {
		cacheLen = int(q.Limit[1])
	}

	result, err := scanMapList(rows, flat, q.SelectColLinkStr, cacheLen)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func scanMapList(rows *sql.Rows, flat bool, colLinkStr string, cacheLen int) (result []map[string]interface{}, err error) {
	if rows != nil {
		defer func(rows *sql.Rows) {
			err = rows.Close()
			if err != nil {
				log.Println(err.Error())
			}
		}(rows)
	} else {
		return nil, nil
	}

	cols, err := rows.Columns()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	colType, err := rows.ColumnTypes()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	if cacheLen <= 0 {
		cacheLen = defCacheSize
	}

	result = make([]map[string]interface{}, 0, cacheLen)
	for {
		b := rows.Next()
		if !b {
			break
		}
		row := make([]interface{}, len(cols))
		prepareValues(row, colType, cols)
		err = rows.Scan(row...)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		m := map[string]interface{}{}
		scanIntoMap(m, row, cols)
		if len(m) > 0 && !flat {
			formatMap(m, colLinkStr)
		}
		result = append(result, m)
	}

	return result, nil
}

func toFirstMap(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, flat bool) (map[string]interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	q.Limit = []uint{1}
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

	result, err := scanMapList(rows, flat, q.SelectColLinkStr, 1)
	if err != nil {
		return nil, err
	}

	if len(result) <= 0 {
		return nil, nil
	}

	return result[0], nil
}

func setDataFunc(dataVal reflect.Value, v interface{}) error {
	val := fmt.Sprintf("%v", v)
	switch v := v.(type) {
	case string:
		dataVal.SetString(val)
	case int, int8, int16, int32, int64:
		i64, err := util.Str2Int[int64](val)
		if err != nil {
			return err
		}
		dataVal.SetInt(i64)
	case uint, uint8, uint16, uint32, uint64:
		u64, err := util.Str2Uint[uint64](val)
		if err != nil {
			return err
		}
		dataVal.SetUint(u64)
	case float32, float64:
		f64, err := util.Str2Float[float64](val)
		if err != nil {
			return err
		}
		dataVal.SetFloat(f64)
	case []byte:
		dataVal.SetBytes(v)
	default:
		dataVal.Set(reflect.ValueOf(v))
	}

	return nil
}

func jsonListDataFormat(elem reflect.Type, ret []map[string]interface{}) error {
	if elem.Kind() == reflect.Struct || (elem.Kind() == reflect.Ptr || elem.Elem().Kind() == reflect.Struct) {
		colMap, err := structJSONField(elem)
		if err != nil {
			return err
		}

		err = formatListMapData(ret, colMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func jsonDataFormat(elem reflect.Type, ret map[string]interface{}) error {
	if elem.Kind() == reflect.Struct {
		colMap, err := structJSONField(elem)
		if err != nil {
			return err
		}

		err = formatMapData(ret, colMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func toListData(ctx context.Context, db *DB, tx *Tx, q *BaseQuery, result interface{}, flat bool, dataValue *reflect.Value) error {
	elemType := dataValue.Type().Elem()
	if elemType.Kind() == reflect.Struct || elemType.Kind() == reflect.Map ||
		(elemType.Kind() == reflect.Ptr && (elemType.Elem().Kind() == reflect.Struct ||
			elemType.Elem().Kind() == reflect.Map)) {
		ret, err := toListMap(ctx, db, tx, q, flat)
		if err != nil {
			return err
		}
		err = jsonListDataFormat(elemType, ret)
		if err != nil {
			return err
		}

		if (elemType.Kind() == reflect.Map && elemType.Elem().Kind() == reflect.Interface) ||
			(elemType.Kind() == reflect.Ptr && elemType.Elem().Kind() == reflect.Map &&
				elemType.Elem().Elem().Kind() == reflect.Interface) {
			dataValue.Set(reflect.ValueOf(ret))
			return err
		}

		err = util.Interface2Interface(ret, result)
		if err != nil {
			return err
		}
	} else {
		ret, err := toListMap(ctx, db, tx, q, true)
		if err != nil {
			return err
		}
		if len(ret) <= 0 {
			return nil
		}

		if len(ret[0]) != 1 {
			return fmt.Errorf("column too many")
		}

		mapKey := ""
		for k := range ret[0] {
			mapKey = k
			break
		}

		elemList := reflect.MakeSlice(reflect.SliceOf(elemType), len(ret), len(ret))
		//for _, m := range ret {
		//	newData := reflect.New(elemType)
		//	newData = newData.Elem()
		//	err = setDataFunc(newData, m[mapKey])
		//	if err != nil {
		//		return err
		//	}
		//	elemList = reflect.Append(elemList, newData)
		//}

		for i := range ret {
			err = setDataFunc(elemList.Index(i), ret[i][mapKey])
			if err != nil {
				return err
			}
			// elemList = reflect.Append(elemList, newData)
		}

		dataValue.Set(elemList)
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
	if dataValue.Type().Kind() == reflect.Slice {
		return toListData(ctx, db, tx, q, result, flat, &dataValue)
	} else if dataValue.Type().Kind() == reflect.Map || dataValue.Type().Kind() == reflect.Struct {
		ret, err := toFirstMap(ctx, db, tx, q, flat)
		if err != nil {
			return err
		}
		err = jsonDataFormat(dataValue.Type(), ret)
		if err != nil {
			return err
		}

		if dataValue.Type().Kind() == reflect.Map && dataValue.Type().Elem().Kind() == reflect.Interface {
			dataValue.Set(reflect.ValueOf(ret))
			return nil
		}
		err = util.Interface2Interface(ret, result)
		if err != nil {
			return err
		}
	} else {
		ret, err := toFirstMap(ctx, db, tx, q, true)
		if err != nil {
			return err
		}
		if len(ret) <= 0 {
			return nil
		}

		if len(ret) != 1 {
			return fmt.Errorf("column too many")
		}
		for _, v := range ret {
			err = setDataFunc(dataValue, v)
			if err != nil {
				return err
			}
			break
		}
	}

	return nil
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

func TransSession(ctx context.Context, db *DB, f func(ctx context.Context, tx *Tx) error) (err error) {
	tx, errTx := db.BeginTx(ctx, nil)
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

func structJSONField(dataType reflect.Type) (map[string]interface{}, error) {
	structMap := map[string]interface{}{}
	return structJSONKey(dataType, structMap)
}

func structJSONKey(dataType reflect.Type, structMap map[string]interface{}) (map[string]interface{}, error) {
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if dataType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("数据必须为：struct、*struct")
	}

	structPath := fmt.Sprintf("%s.%s", dataType.PkgPath(), dataType.Name())
	if data, ok := structMap[structPath]; ok {
		return data.(map[string]interface{}), nil
	}

	colMap := map[string]interface{}{}
	structMap[structPath] = colMap

	for i := 0; i < dataType.NumField(); i++ {
		tagName := dataType.Field(i).Tag.Get(structTypeName)
		colName := dataType.Field(i).Tag.Get("json")
		ref := dataType.Field(i).Tag.Get("ref")
		if tagName == structTypeValue {
			if dataType.Field(i).Type.Kind() == reflect.Slice {
				colMap[colName] = jsonSlice
			} else if dataType.Field(i).Type.Kind() == reflect.String {
				colMap[colName] = jsonStr
			} else if dataType.Field(i).Type.Kind() == reflect.Map || dataType.Field(i).Type.Kind() == reflect.Struct ||
				(dataType.Field(i).Type.Kind() == reflect.Ptr && (dataType.Field(i).Type.Elem().Kind() == reflect.Map ||
					dataType.Field(i).Type.Elem().Kind() == reflect.Struct)) {
				colMap[colName] = jsonMap
			} else {
				colMap[colName] = jsonOther
			}
			continue
		}

		if ref != "" && dataType.Field(i).IsExported() {
			val, err := structJSONKey(dataType.Field(i).Type, structMap)
			if err != nil {
				return nil, err
			}

			colMap[colName] = val
		}
	}

	return colMap, nil
}

func formatMapData(data map[string]interface{}, colMap map[string]interface{}) error {
	if len(colMap) <= 0 {
		return nil
	}

	for k, v := range data {
		if v == nil {
			continue
		}

		if vv, ok := v.(map[string]interface{}); ok {
			m := colMap[k]
			if m == nil {
				continue
			} else if _, ok = m.(structJSONType); ok {
				continue
			}

			if mm, ok := m.(map[string]interface{}); ok {
				err := formatMapData(vv, mm)
				if err != nil {
					return err
				}
			}
		}

		if colData, ok := colMap[k]; ok {
			tp, ok := colData.(structJSONType)
			if !ok {
				data[k] = v
				continue
			}

			val := v.(string)
			switch tp {
			case jsonSlice:
				if val == "" {
					v = []interface{}{}
				} else {
					err := json.Unmarshal([]byte(val), &v)
					if err != nil {
						data[k] = nil
						log.Println(err)
						return err
					}
				}
			case jsonMap:
				if val == "" {
					v = map[string]interface{}{}
				} else {
					err := json.Unmarshal([]byte(val), &v)
					if err != nil {
						data[k] = nil
						log.Println(err)
						return err
					}
				}
			case jsonStr:
				v = val
			default:
				err := json.Unmarshal([]byte(val), &v)
				if err != nil {
					data[k] = nil
					log.Println(err)
					return err
				}
			}
			data[k] = v
		}
	}
	return nil
}

func formatListMapData(data []map[string]interface{}, colMap map[string]interface{}) error {
	if len(colMap) <= 0 {
		return nil
	}

	for _, elem := range data {
		err := formatMapData(elem, colMap)
		if err != nil {
			return err
		}
	}

	return nil
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
				m[colName] = util.InterfaceToString(dataValue.Field(i).Interface())
			} else {
				m[colName] = dataValue.Field(i).Interface()
			}
		}
	}
	return m
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
