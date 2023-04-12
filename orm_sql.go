package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
	"github.com/assembly-hub/task/execute"

	"github.com/assembly-hub/orm/dbtype"
)

func (orm *ORM) formatInsertSQL(data interface{}) (string, error) {
	return orm.innerInsertOrReplaceSQL("insert", data)
}

// 需要主键
func (orm *ORM) formatUpdateSQL(data interface{}) (string, error) {
	return orm.innerUpdateSQL(data)
}

func (orm *ORM) formatInsertManySQL(dataList []interface{}, cols []string) (string, error) {
	switch orm.ref.dbConf.DBType {
	case dbtype.MySQL, dbtype.MariaDB, dbtype.SQLServer, dbtype.SQLite2, dbtype.SQLite3, dbtype.Postgres, dbtype.OpenGauss:
		return orm.innerInsertOrReplaceManySQL("insert", dataList, cols)
	case dbtype.Oracle:
		return orm.oracleInsertManySQL(dataList, cols)
	default:
		return "", ErrDBType
	}
}

func (orm *ORM) formatReplaceSQL(data interface{}) (string, error) {
	switch orm.ref.dbConf.DBType {
	case dbtype.MySQL, dbtype.MariaDB, dbtype.SQLite2, dbtype.SQLite3:
		return orm.innerInsertOrReplaceSQL("replace", data)
	case dbtype.Oracle, dbtype.SQLServer, dbtype.Postgres, dbtype.OpenGauss:
		return "", fmt.Errorf("当前数据库不支持Replace方法，请使用Upsert方法")
	default:
		return "", ErrDBType
	}
}

func (orm *ORM) formatReplaceManySQL(dataList []interface{}, cols []string) (string, error) {
	switch orm.ref.dbConf.DBType {
	case dbtype.MySQL, dbtype.MariaDB, dbtype.SQLite2, dbtype.SQLite3:
		return orm.innerInsertOrReplaceManySQL("replace", dataList, cols)
	case dbtype.Oracle, dbtype.SQLServer, dbtype.Postgres, dbtype.OpenGauss:
		return "", fmt.Errorf("当前数据库不支持Replace方法，请使用Upsert方法")
	default:
		return "", ErrDBType
	}
}

func (orm *ORM) formatUpsertSQL(data interface{}) (string, error) {
	switch orm.ref.dbConf.DBType {
	case dbtype.MySQL, dbtype.MariaDB:
		return orm.mysqlUpsertSQL(data)
	case dbtype.OpenGauss:
		return orm.gaussUpsertSQL(data)
	case dbtype.SQLite2, dbtype.SQLite3, dbtype.Postgres:
		return orm.sqliteUpsertSQL(data)
	case dbtype.SQLServer:
		return orm.sqlserverUpsertSQL(data)
	case dbtype.Oracle:
		return orm.oracleUpsertSQL(data)
	default:
		return "", ErrDBType
	}
}

func (orm *ORM) formatUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
	switch orm.ref.dbConf.DBType {
	case dbtype.MySQL, dbtype.MariaDB:
		return orm.mysqlUpsertManySQL(dataList, cols)
	case dbtype.OpenGauss:
		return orm.gaussUpsertManySQL(dataList, cols)
	case dbtype.SQLite2, dbtype.SQLite3, dbtype.Postgres:
		return orm.sqliteUpsertManySQL(dataList, cols)
	case dbtype.SQLServer:
		return orm.sqlserverUpsertManySQL(dataList, cols)
	case dbtype.Oracle:
		return orm.oracleUpsertManySQL(dataList, cols)
	default:
		return "", ErrDBType
	}
}

func (orm *ORM) formatSaveSQL(data interface{}) (string, error) {
	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"

	var valMap map[string]interface{}
	switch data := data.(type) {
	case map[string]interface{}:
		valMap = data
	default:
		dataValue := reflect.ValueOf(data)
		if dataValue.Type().Kind() != reflect.Struct && dataValue.Type().Kind() != reflect.Ptr {
			return "", fmt.Errorf(typeErrStr)
		}

		if dataValue.Type().Kind() == reflect.Ptr {
			dataValue = dataValue.Elem()
		}

		if dataValue.Type().Kind() != reflect.Struct {
			return "", fmt.Errorf(typeErrStr)
		}

		valMap = map[string]interface{}{}

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			valMap[colName] = dataValue.Field(i).Interface()
		}
	}
	if len(valMap) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	if pk, ok := valMap[orm.primaryKey]; ok && pk != nil && pk != "" && pk != "0" {
		return orm.formatUpdateSQL(valMap)
	}
	return orm.formatInsertSQL(data)
}

func (orm *ORM) formatValue(raw interface{}) (ret string, timeEmpty bool) {
	ret, timeEmpty = "", false
	if raw == nil {
		ret = "null"
		return
	}

	var strBuf strings.Builder

	switch raw := raw.(type) {
	case string:
		ret = raw
		ret = strings.ReplaceAll(ret, "'", "''")
		strBuf.Grow(len(ret) + 2)
		strBuf.WriteByte('\'')
		strBuf.WriteString(ret)
		strBuf.WriteByte('\'')
		ret = strBuf.String()
	case time.Time:
		if raw.IsZero() {
			timeEmpty = true
			break
		}

		ret = time2Str(raw)
		strBuf.Grow(len(ret) + 2)
		strBuf.WriteByte('\'')
		strBuf.WriteString(ret)
		strBuf.WriteByte('\'')
		ret = strBuf.String()
		if orm.ref.dbConf.DBType == dbtype.Oracle {
			ret = oracleDateTime(ret, false)
		}
	case *time.Time:
		if raw.IsZero() {
			timeEmpty = true
			break
		}

		ret = time2Str(*raw)
		strBuf.Grow(len(ret) + 2)
		strBuf.WriteByte('\'')
		strBuf.WriteString(ret)
		strBuf.WriteByte('\'')
		ret = strBuf.String()
		if orm.ref.dbConf.DBType == dbtype.Oracle {
			ret = oracleDateTime(ret, false)
		}
	case bool:
		if raw {
			ret = "1"
		} else {
			ret = "0"
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		ret = util.InterfaceToString(raw)
	default:
		def := reflect.ValueOf(raw)
		if def.Type().Kind() == reflect.String {
			ret = def.String()
		} else {
			ret = util.InterfaceToString(raw)
		}

		ret = strings.ReplaceAll(ret, "'", "''")
		strBuf.Grow(len(ret) + 2)
		strBuf.WriteByte('\'')
		strBuf.WriteString(ret)
		strBuf.WriteByte('\'')
		ret = strBuf.String()
	}
	return
}

func (orm *ORM) checkUK(colSet set.Set[string]) bool {
	if orm.uniqueKeys.Empty() {
		return false
	}

	uk := orm.uniqueKeys.ToList()
	for _, k := range uk {
		if !colSet.Has(k) {
			return false
		}
	}
	return true
}

func (orm *ORM) batchExecuteSQL(sqlArr []interface{}, trans bool) (affected int64, err error) {
	sqlTask := execute.NewExecutor("batchExecuteSQL task")

	if trans && orm.db != nil {
		tx, errTx := orm.db.BeginTx(orm.ctx, nil)
		if errTx != nil {
			return 0, errTx
		}
		defer func() {
			if p := recover(); p != nil {
				err1 := tx.Rollback()
				err = fmt.Errorf("%v, Rollback=%w", p, err1)
			}
		}()

		for _, sqlObj := range sqlArr {
			sqlTask.AddSimpleTask(func(sqlStr string) (int64, error) {
				execContext, err := tx.ExecContext(orm.ctx, sqlStr)
				if err != nil {
					return 0, err
				}

				return execContext.RowsAffected()
			}, sqlObj.(string))
		}

		retList, taskErrList, err := sqlTask.ExecuteTaskWithErr()
		if err != nil {
			panic(err)
		}

		for i, e := range taskErrList {
			if e != nil {
				panic(e)
			}
			affected += retList[i].(int64)
		}

		err = tx.Commit()
	} else {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("%v", p)
			}
		}()

		var execContext sql.Result
		for _, sqlObj := range sqlArr {
			sqlTask.AddSimpleTask(func(sqlStr string) (int64, error) {
				if orm.tx != nil {
					execContext, err = orm.tx.ExecContext(orm.ctx, sqlStr)
				} else if orm.db != nil {
					execContext, err = orm.db.ExecContext(orm.ctx, sqlStr)
				} else {
					return 0, ErrClient
				}
				if err != nil {
					return 0, err
				}

				return execContext.RowsAffected()
			}, sqlObj.(string))
		}

		retList, taskErrList, err := sqlTask.ExecuteTaskWithErr()
		if err != nil {
			panic(err)
		}

		for i, e := range taskErrList {
			if e != nil {
				panic(e)
			}
			affected += retList[i].(int64)
		}
	}
	return
}