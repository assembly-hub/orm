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
	"github.com/assembly-hub/db"
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

func prepareValues(values []interface{}, columnTypes []db.ColumnType, columns []string) {
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

func newDataByDBType(columnType db.ColumnType) interface{} {
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
			tbStruct := q.RefConf.getTableCacheByTp(tempField.DataType)
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

func count(ctx context.Context, sqlDB db.BaseExecutor, q *BaseQuery) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var rows db.Rows
	var err error
	if sqlDB != nil {
		rows, err = sqlDB.QueryContext(ctx, q.Count())
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
