// package orm
package orm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/assembly-hub/basics/util"
)

const (
	selectColLinkStr  = "__"
	defaultPrimaryKey = "id"
	defaultBatchSize  = 200
)

type mysqlQuery struct {
	Distinct        bool
	SelectForUpdate bool
	Select          []string
	Order           []string
	Limit           []uint
	Where           map[string]interface{}
	GroupBy         []string
	Having          map[string]interface{}
}

func newMySQLQuery() *mysqlQuery {
	q := new(mysqlQuery)
	q.Distinct = false
	q.SelectForUpdate = false
	q.Select = []string{}
	q.Order = []string{}
	q.Limit = []uint{}
	q.Where = map[string]interface{}{}
	q.GroupBy = []string{}
	q.Having = map[string]interface{}{}
	return q
}

// where 条件 支持子查询 MySqlQuery or *MySqlQuery
// = : "key": "val" or "key__eq": "val" or "key__bin_eq": "val"
// < : "key__lt": 1 or "key__bin_lt": 1
// <= : "key__lte": 1 or "key__bin_lte": 1
// > : "key__gt": 1 or "key__bin_gt": 1
// >= : "key__gte": 1 or "key__bin_gte": 1
// != : "key__ne": 1 or "key__bin_ne": 1
// in : "key__in": [1] or "key__bin_in": [1]
// not in : "key__nin": [1] or "key__bin_nin": [1]
// date : "key__date": "2022-01-01"
// between : "key__between": [1, 2]

// 以下不支持子查询
// is null : "key__null": true
// is not null : "key__null": false
// $or : map[string]interface{} or []map[string]interface{}
// $and : map[string]interface{} or []map[string]interface{}
// and_like :
//		"key__istartswith": "123"
//		"key__startswith": "123"
//		"key__iendswith": "123"
//		"key__endswith": "123"
//		"key__icontains": "123" or ["123", "123"]
//		"key__contains": "123" or ["123", "123"]
// or_like :
//		"key__or_istartswith": "123" or ["123", "123"]
//		"key__or_startswith": "123" or ["123", "123"]
//		"key__or_iendswith": "123" or ["123", "123"]
//		"key__or_endswith": "123" or ["123", "123"]
//		"key__or_icontains": "123" or ["123", "123"]
//		"key__or_contains": "123" or ["123", "123"]

// ORM
// # 为内置符号，标志为原始字段，不进行任何处理，仅在以下数据有效：
// Select Order GroupBy Where Having
type ORM struct {
	tableName string
	db        *DB
	tx        *Tx
	ref       *Reference

	ctx        context.Context
	primaryKey string

	// 字段别名链接字符串
	selectColLinkStr string
	// 保留上次查询参数
	keepQuery bool

	// 查询配置数据
	Q *mysqlQuery
}

type Paging struct {
	PageNo    int `json:"page_no"`    //当前页
	PageSize  int `json:"page_size"`  //每页条数
	Total     int `json:"total"`      //总条数
	PageTotal int `json:"page_total"` //总页数
}

func NewORM(ctx context.Context, tableName string, db *DB, ref *Reference) *ORM {
	err := globalVerifyObj.VerifyTableName(tableName)
	if err != nil {
		panic(err)
	}
	dao := new(ORM)
	dao.tableName = tableName
	dao.keepQuery = true
	dao.db = db
	dao.ref = ref
	dao.selectColLinkStr = "_"
	dao.Q = newMySQLQuery()
	dao.ctx = ctx
	dao.primaryKey = defaultPrimaryKey
	return dao
}

func NewORMWithTx(ctx context.Context, tableName string, tx *Tx, ref *Reference) *ORM {
	err := globalVerifyObj.VerifyTableName(tableName)
	if err != nil {
		panic(err)
	}

	dao := new(ORM)
	dao.tableName = tableName
	dao.db = nil
	dao.keepQuery = true
	dao.tx = tx
	dao.ref = ref
	dao.selectColLinkStr = "_"
	dao.Q = newMySQLQuery()
	dao.ctx = ctx
	dao.primaryKey = defaultPrimaryKey
	return dao
}

func (orm *ORM) SelectColLinkStr(s string) *ORM {
	orm.selectColLinkStr = s
	return orm
}

// Query 条件对
// "id__gt", 1, "name": "test"
func (orm *ORM) Query(pair ...interface{}) *ORM {
	if len(pair)%2 != 0 {
		panic("pair长度必须是2的整数倍")
	}
	if len(pair) <= 0 {
		return orm
	}

	for i, n := 0, len(pair)/2; i < n; i++ {
		if pair[i*2] == nil || pair[i*2] == "" || pair[i*2+1] == nil {
			panic("Fields and conditions cannot be nil")
		}

		switch v := pair[i*2+1].(type) {
		case *ORM:
			orm.Q.Where[util.InterfaceToString(pair[i*2])] = v.cond(false)
		default:
			orm.Q.Where[util.InterfaceToString(pair[i*2])] = v
		}
	}
	return orm
}

func (orm *ORM) Clone() *ORM {
	dao := new(ORM)
	dao.tableName = orm.tableName
	dao.db = orm.db
	dao.tx = orm.tx
	dao.ref = orm.ref
	dao.selectColLinkStr = orm.selectColLinkStr
	dao.Q = newMySQLQuery()
	dao.ctx = context.Background()
	dao.primaryKey = orm.primaryKey
	return dao
}

func (orm *ORM) PrimaryKey(k string) *ORM {
	orm.primaryKey = k
	return orm
}

func (orm *ORM) KeepQuery(b bool) *ORM {
	orm.keepQuery = b
	return orm
}

func (orm *ORM) OverLimit(over, size uint) *ORM {
	orm.Q.Limit = []uint{over, size}
	return orm
}

func (orm *ORM) Page(pageNo, pageSize uint) *ORM {
	if pageSize <= 0 {
		panic("page size must be gt 0")
	}
	if pageNo <= 0 {
		panic("page no must be gt 0")
	}

	orm.Q.Limit = []uint{pageSize * (pageNo - 1), pageSize}
	return orm
}

func (orm *ORM) Limit(size uint) *ORM {
	orm.Q.Limit = []uint{size}
	return orm
}

func (orm *ORM) Distinct(b bool) *ORM {
	orm.Q.Distinct = b
	return orm
}

func (orm *ORM) SelectForUpdate(b bool) *ORM {
	orm.Q.SelectForUpdate = b
	return orm
}

func (orm *ORM) Where(col string, value interface{}) *ORM {
	if col == "" || value == nil {
		panic("Fields and conditions cannot be nil")
	}

	switch v := value.(type) {
	case *ORM:
		orm.Q.Where[col] = v.cond(false)
	default:
		orm.Q.Where[col] = v
	}

	return orm
}

func (orm *ORM) Wheres(where Where) *ORM {
	for col, value := range where {
		if col == "" || value == nil {
			panic("Fields and conditions cannot be nil")
		}

		switch v := value.(type) {
		case *ORM:
			orm.Q.Where[col] = v.cond(false)
		default:
			orm.Q.Where[col] = v
		}
	}
	return orm
}

// Select 参数：* 主表所有字段；tag.* tag对应表所有字段；tag1.tag2.* tag1表的tag2的所有字段；以此类推
// *0 等价 * 只考虑主表，不展开子表
// *1 对应层级的表展开一层（主表+一级关联表，二级以下联表不算）
// *2 对应层级的表展开二层（主表+一级关联表+二级关联表，三级以下联表不算）
// *n 对应层级的表展开n层（主表+1级关联表+2级关联表+...+n级关联表，n+1级以下联表不算）
// 上述法则同样适用于tag.*1，此时的主表为tag，以此类推 tag1.tag2.*1，主表为tag1.tag2
// 字段排除：优先级最高
// -name：删除name字段；-* 移除主表字段; -tag.name:删除tag表对应的name字段；-tag.*：删除整个tag表所有字段
// 排除字段也支持 *n 语法，-*n
// *n与- 均不可与 # 混用
func (orm *ORM) Select(cols ...string) *ORM {
	orm.Q.Select = append(orm.Q.Select, cols...)
	return orm
}

func (orm *ORM) Order(cols ...string) *ORM {
	orm.Q.Order = append(orm.Q.Order, cols...)
	return orm
}

func (orm *ORM) GroupBy(cols ...string) *ORM {
	orm.Q.GroupBy = append(orm.Q.GroupBy, cols...)
	return orm
}

func (orm *ORM) ClearCache() *ORM {
	orm.Q = newMySQLQuery()
	return orm
}

func (orm *ORM) Having(col string, value interface{}) *ORM {
	if col == "" || value == nil {
		panic("Fields and conditions cannot be nil")
	}

	switch v := value.(type) {
	case *ORM:
		orm.Q.Having[col] = v.cond(false)
	default:
		orm.Q.Having[col] = v
	}
	return orm
}

func (orm *ORM) HavingSome(where Having) *ORM {
	for col, value := range where {
		if col == "" || value == nil {
			panic("Fields and conditions cannot be nil")
		}

		switch v := value.(type) {
		case *ORM:
			orm.Q.Having[col] = v.cond(false)
		default:
			orm.Q.Having[col] = v
		}
	}
	return orm
}

func (orm *ORM) cond(flat bool) *BaseQuery {
	q := BaseQuery{
		RefConf:          orm.ref,
		TableName:        orm.tableName,
		Where:            orm.Q.Where,
		SelectColLinkStr: orm.selectColLinkStr,
		Order:            orm.Q.Order,
		Distinct:         orm.Q.Distinct,
		SelectForUpdate:  orm.Q.SelectForUpdate,
		Limit:            orm.Q.Limit,
		Select:           orm.Q.Select,
		GroupBy:          orm.Q.GroupBy,
		Having:           orm.Q.Having,
	}
	if !flat {
		q.SelectColLinkStr = selectColLinkStr
	}
	return &q
}

func (orm *ORM) ToSQL(flat bool) string {
	q := BaseQuery{
		RefConf:          orm.ref,
		TableName:        orm.tableName,
		Where:            orm.Q.Where,
		SelectColLinkStr: orm.selectColLinkStr,
		Order:            orm.Q.Order,
		Distinct:         orm.Q.Distinct,
		SelectForUpdate:  orm.Q.SelectForUpdate,
		Limit:            orm.Q.Limit,
		Select:           orm.Q.Select,
		GroupBy:          orm.Q.GroupBy,
		Having:           orm.Q.Having,
	}
	if !flat {
		q.SelectColLinkStr = selectColLinkStr
	}

	return q.SQL()
}

func (orm *ORM) PageData(result interface{}, flat bool, pageNo, pageSize uint) (*Paging, error) {
	totalCount, err := orm.Count(false)
	if err != nil {
		return nil, err
	}

	if pageNo == 0 || pageSize == 0 {
		return nil, fmt.Errorf("page no page size need gt 0")
	}

	totalPage := totalCount / int64(pageSize)
	if totalCount%int64(pageSize) > 0 {
		totalPage++
	}

	if pageNo > uint(totalPage) {
		pageNo = uint(totalPage)
	}
	if pageNo < 1 {
		pageNo = 1
	}

	orm.Page(pageNo, pageSize)

	err = orm.ToData(result, flat)
	if err != nil {
		return nil, err
	}

	p := &Paging{
		PageNo:    int(pageNo),
		PageSize:  int(pageSize),
		Total:     int(totalCount),
		PageTotal: int(totalPage),
	}
	return p, nil
}

func (orm *ORM) ToData(result interface{}, flat bool) error {
	if !orm.keepQuery {
		defer func() {
			orm.ClearCache()
		}()
	}

	q := BaseQuery{
		RefConf:          orm.ref,
		TableName:        orm.tableName,
		Where:            orm.Q.Where,
		SelectColLinkStr: orm.selectColLinkStr,
		Order:            orm.Q.Order,
		Distinct:         orm.Q.Distinct,
		SelectForUpdate:  orm.Q.SelectForUpdate,
		Limit:            orm.Q.Limit,
		Select:           orm.Q.Select,
		GroupBy:          orm.Q.GroupBy,
		Having:           orm.Q.Having,
	}
	if !flat {
		q.SelectColLinkStr = selectColLinkStr
	}

	return toData(orm.ctx, orm.db, orm.tx, &q, result, flat)
}

// Exist 检查数据是否存在
func (orm *ORM) Exist() (bool, error) {
	if !orm.keepQuery {
		defer func() {
			orm.ClearCache()
		}()
	}

	q := BaseQuery{
		RefConf:          orm.ref,
		TableName:        orm.tableName,
		Where:            orm.Q.Where,
		SelectColLinkStr: orm.selectColLinkStr,
		Order:            orm.Q.Order,
		Distinct:         orm.Q.Distinct,
		SelectForUpdate:  orm.Q.SelectForUpdate,
		Limit:            Limit{1},
		Select:           Select{"id"},
		GroupBy:          orm.Q.GroupBy,
		Having:           orm.Q.Having,
	}

	var c int64
	err := toData(orm.ctx, orm.db, orm.tx, &q, &c, false)
	if err != nil {
		return false, err
	}

	if c > 0 {
		return true, nil
	}
	return false, nil
}

func (orm *ORM) Count(clearCache bool) (int64, error) {
	if clearCache {
		defer func() {
			orm.ClearCache()
		}()
	}
	q := BaseQuery{
		RefConf:          orm.ref,
		TableName:        orm.tableName,
		Where:            orm.Q.Where,
		SelectColLinkStr: selectColLinkStr,
		Order:            orm.Q.Order,
		Distinct:         orm.Q.Distinct,
		SelectForUpdate:  orm.Q.SelectForUpdate,
		Limit:            []uint{1},
		Select:           orm.Q.Select,
		GroupBy:          orm.Q.GroupBy,
		Having:           orm.Q.Having,
	}

	return count(orm.ctx, orm.db, orm.tx, &q)
}

func (orm *ORM) InsertOne(data interface{}) (insertID int64, err error) {
	insertSQL, err := orm.formatInsertSQL(data)
	if err != nil {
		return 0, err
	}

	var ret sql.Result
	if orm.tx != nil {
		ret, err = orm.tx.ExecContext(orm.ctx, insertSQL)
	} else if orm.db != nil {
		ret, err = orm.db.ExecContext(orm.ctx, insertSQL)
	} else {
		return 0, ErrClient
	}
	if err != nil {
		return 0, err
	}
	id, err := ret.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (orm *ORM) InsertMany(data []interface{}, trans bool) (affected int64, insertIds []int64, err error) {
	if len(data) <= 0 {
		return 0, nil, fmt.Errorf("no data")
	}
	var sqlArr []string
	for _, d := range data {
		insertSQL, err := orm.formatInsertSQL(d)
		if err != nil {
			return 0, nil, err
		}
		sqlArr = append(sqlArr, insertSQL)
	}

	if len(sqlArr) <= 0 {
		return 0, nil, fmt.Errorf("no data sql")
	}

	if trans && orm.db != nil {
		tx, errTx := orm.db.BeginTx(orm.ctx, nil)
		if errTx != nil {
			return 0, nil, errTx
		}
		defer func() {
			if p := recover(); p != nil {
				err1 := tx.Rollback()
				err = fmt.Errorf("%v, Rollback=%w", p, err1)
			}
		}()

		for _, sqlObj := range sqlArr {
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			insertID, err := execContext.LastInsertId()
			if err != nil {
				panic(err)
			}
			insertIds = append(insertIds, insertID)

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, nil, ErrClient
			}

			if err != nil {
				panic(err)
			}

			insertID, err := execContext.LastInsertId()
			if err != nil {
				panic(err)
			}
			insertIds = append(insertIds, insertID)

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

// InsertManySameClos
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) InsertManySameClos(data []interface{}, cols []string, batchSize int, trans bool) (affected int64, err error) {
	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	if len(cols) <= 0 {
		return 0, fmt.Errorf("no cols")
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	dataArr := util.ArrSplit(data, batchSize)

	var sqlArr []string

	for _, d := range dataArr {
		insertSQL, err := orm.formatInsertManySQL(d, cols)
		if err != nil {
			return 0, err
		}

		sqlArr = append(sqlArr, insertSQL)
	}

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
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, ErrClient
			}
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

func (orm *ORM) UpsertOne(data interface{}) (insertID int64, err error) {
	insertSQL, err := orm.formatUpsertSQL(data)
	if err != nil {
		return 0, err
	}

	var ret sql.Result
	if orm.tx != nil {
		ret, err = orm.tx.ExecContext(orm.ctx, insertSQL)
	} else if orm.db != nil {
		ret, err = orm.db.ExecContext(orm.ctx, insertSQL)
	} else {
		return 0, ErrClient
	}
	if err != nil {
		return 0, err
	}
	insertID, err = ret.LastInsertId()
	if err != nil {
		return 0, err
	}
	return insertID, nil
}

func (orm *ORM) UpsertMany(data []interface{}, trans bool) (affected int64, insertIDs []int64, err error) {
	if len(data) <= 0 {
		return 0, nil, fmt.Errorf("no data")
	}
	var sqlArr []string
	for _, d := range data {
		insertSQL, err := orm.formatUpsertSQL(d)
		if err != nil {
			return 0, nil, err
		}
		sqlArr = append(sqlArr, insertSQL)
	}

	if len(sqlArr) <= 0 {
		return 0, nil, fmt.Errorf("no data sql")
	}

	if trans && orm.db != nil {
		tx, errTx := orm.db.BeginTx(orm.ctx, nil)
		if errTx != nil {
			return 0, nil, errTx
		}
		defer func() {
			if p := recover(); p != nil {
				err1 := tx.Rollback()
				err = fmt.Errorf("%v, Rollback=%w", p, err1)
			}
		}()

		for _, sqlObj := range sqlArr {
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			insertID, err := execContext.LastInsertId()
			if err != nil {
				panic(err)
			}
			insertIDs = append(insertIDs, insertID)

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, nil, ErrClient
			}
			if err != nil {
				panic(err)
			}

			insertID, err := execContext.LastInsertId()
			if err != nil {
				panic(err)
			}
			insertIDs = append(insertIDs, insertID)

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

// UpsertManySameClos
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) UpsertManySameClos(data []interface{}, cols []string, batchSize int, trans bool) (affected int64, err error) {
	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	if len(cols) <= 0 {
		return 0, fmt.Errorf("no cols")
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	dataArr := util.ArrSplit(data, batchSize)

	var sqlArr []string

	for _, d := range dataArr {
		upsertSQL, err := orm.formatUpsertManySQL(d, cols)
		if err != nil {
			return 0, err
		}

		sqlArr = append(sqlArr, upsertSQL)
	}

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
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, ErrClient
			}
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

// SaveMany
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) SaveMany(data []interface{}, trans bool) (affected int64, err error) {
	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	var sqlArr []string

	for _, d := range data {
		upsertSQL, err := orm.formatSaveManySQL(d)
		if err != nil {
			return 0, err
		}

		sqlArr = append(sqlArr, upsertSQL)
	}

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
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, ErrClient
			}
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

func (orm *ORM) UpdateByWhere(update map[string]interface{}, where Where) (affected int64, err error) {
	affected, err = 0, nil

	updateSQL := "update `%s` set %s"

	if update == nil {
		return 0, fmt.Errorf("update data is nil")
	}

	if len(where) > 0 {
		q := BaseQuery{
			RefConf:   orm.ref,
			TableName: orm.tableName,
			Where:     where,
		}
		updateSQL += " where " + q.GetWhere()
	}

	updateSet := []string{}
	for k, v := range update {
		var val string
		if k[0] == '#' {
			k = k[1:]
			val = fmt.Sprintf("%v", v)
		} else {
			value, timeEmpty := formatValue(v)
			if timeEmpty {
				val = "null"
			} else {
				val = value
			}
		}

		updateSet = append(updateSet, fmt.Sprintf("`%s`=%s", k, val))
	}

	updateSQL = fmt.Sprintf(updateSQL, orm.tableName, util.JoinArr(updateSet, ","))

	var ret sql.Result
	if orm.tx != nil {
		ret, err = orm.tx.ExecContext(orm.ctx, updateSQL)
	} else if orm.db != nil {
		ret, err = orm.db.ExecContext(orm.ctx, updateSQL)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}
	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

// UpdateMany 主键，id 不能为空，为空将更新失败
func (orm *ORM) UpdateMany(data []interface{}, trans bool) (affected int64, err error) {
	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}
	var sqlArr []string
	for _, d := range data {
		updateSQL, err := orm.formatUpdateSQL(d)
		if err != nil {
			return 0, err
		}
		sqlArr = append(sqlArr, updateSQL)
	}

	if len(sqlArr) <= 0 {
		return 0, fmt.Errorf("no data sql")
	}

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
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}
			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, ErrClient
			}
			if err != nil {
				panic(err)
			}
			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

func (orm *ORM) UpdateOne(data interface{}) (affected int64, err error) {
	updateSQL, err := orm.formatUpdateSQL(data)
	if err != nil {
		return 0, err
	}

	var execContext sql.Result
	if orm.tx != nil {
		execContext, err = orm.tx.ExecContext(orm.ctx, updateSQL)
	} else if orm.db != nil {
		execContext, err = orm.db.ExecContext(orm.ctx, updateSQL)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}
	return execContext.RowsAffected()
}

func (orm *ORM) ReplaceOne(data interface{}) (affected int64, err error) {
	replaceSQL, err := orm.formatReplaceSQL(data)
	if err != nil {
		return 0, err
	}

	var execContext sql.Result
	if orm.tx != nil {
		execContext, err = orm.tx.ExecContext(orm.ctx, replaceSQL)
	} else if orm.db != nil {
		execContext, err = orm.db.ExecContext(orm.ctx, replaceSQL)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}
	return execContext.LastInsertId()
}

func (orm *ORM) ReplaceMany(data []interface{}, trans bool) (affected int64, insertIds []int64, err error) {
	if len(data) <= 0 {
		return 0, nil, fmt.Errorf("no data")
	}
	var sqlArr []string
	for _, d := range data {
		insertSQL, err := orm.formatReplaceSQL(d)
		if err != nil {
			return 0, nil, err
		}
		sqlArr = append(sqlArr, insertSQL)
	}

	if len(sqlArr) <= 0 {
		return 0, nil, fmt.Errorf("no data sql")
	}

	if trans && orm.db != nil {
		tx, errTx := orm.db.BeginTx(orm.ctx, nil)
		if errTx != nil {
			return 0, nil, errTx
		}
		defer func() {
			if p := recover(); p != nil {
				err1 := tx.Rollback()
				err = fmt.Errorf("%v, Rollback=%w", p, err1)
			}
		}()

		for _, sqlObj := range sqlArr {
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			insertID, err := execContext.LastInsertId()
			if err != nil {
				panic(err)
			}
			insertIds = append(insertIds, insertID)

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, nil, ErrClient
			}
			if err != nil {
				panic(err)
			}

			insertID, err := execContext.LastInsertId()
			if err != nil {
				panic(err)
			}
			insertIds = append(insertIds, insertID)

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

// ReplaceManySameClos
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) ReplaceManySameClos(data []interface{}, cols []string, batchSize int, trans bool) (affected int64, err error) {
	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	if len(cols) <= 0 {
		return 0, fmt.Errorf("no cols")
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	dataArr := util.ArrSplit(data, batchSize)

	var sqlArr []string

	for _, d := range dataArr {
		replaceSQL, err := orm.formatReplaceManySQL(d, cols)
		if err != nil {
			return 0, err
		}

		sqlArr = append(sqlArr, replaceSQL)
	}

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
			execContext, err := tx.ExecContext(orm.ctx, sqlObj)
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
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
			if orm.tx != nil {
				execContext, err = orm.tx.ExecContext(orm.ctx, sqlObj)
			} else if orm.db != nil {
				execContext, err = orm.db.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, ErrClient
			}
			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected
		}
	}
	return
}

func (orm *ORM) DeleteByWhere(where map[string]interface{}) (affected int64, err error) {
	affected, err = 0, nil

	s := "delete from `%s`"

	if len(where) > 0 {
		q := BaseQuery{
			RefConf:   orm.ref,
			TableName: orm.tableName,
			Where:     where,
		}
		s += " where " + q.GetWhere()
	}

	s = fmt.Sprintf(s, orm.tableName)

	var ret sql.Result
	if orm.tx != nil {
		ret, err = orm.tx.ExecContext(orm.ctx, s)
	} else if orm.db != nil {
		ret, err = orm.db.ExecContext(orm.ctx, s)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}
	rowsAffected, err := ret.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func (orm *ORM) formatInsertSQL(data interface{}) (string, error) {
	insertSQL := "insert into `%s`(%s) values(%s)"

	if data == nil {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	var values []string

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				cols = append(cols, fmt.Sprintf("`%s`", k))
				values = append(values, fmt.Sprintf("%v", v))
				continue
			}

			err := globalVerifyObj.VerifyFieldName(k)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(v)
			if k == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				cols = append(cols, fmt.Sprintf("`%s`", k))
				values = append(values, val)
			}
		}
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

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			err := globalVerifyObj.VerifyFieldName(colName)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				cols = append(cols, fmt.Sprintf("`%s`", colName))
				values = append(values, val)
			}
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	insertSQL = fmt.Sprintf(insertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	return insertSQL, nil
}

func (orm *ORM) formatInsertManySQL(dataList []interface{}, cols []string) (string, error) {
	insertSQL := "insert into `%s`(%s) values%s"

	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"

	var valArr []string
	for _, data := range dataList {
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

		subVal := "("
		for _, colName := range cols {
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal += "null,"
					continue
				}

				subVal += val + ","
			} else if v, ok = valMap["#"+colName]; ok {
				subVal += fmt.Sprintf("%v,", v)
			} else {
				subVal += "null,"
			}
		}
		subVal = subVal[:len(subVal)-1] + ")"
		valArr = append(valArr, subVal)
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("insert data is empty")
	}

	newCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		newCols = append(newCols, fmt.Sprintf("`%s`", k))
	}
	insertSQL = fmt.Sprintf(insertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	return insertSQL, nil
}

func (orm *ORM) formatUpdateSQL(data interface{}) (string, error) {
	updateSQL := "update `%s` set %s where `%s`=%s"

	if data == nil {
		return "", fmt.Errorf("update data is nil")
	}

	typeErrStr := "type of update data is []map[string]interface{} or []*struct or []struct"
	var upSet []string

	primaryVal := ""

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}

				upSet = append(upSet, fmt.Sprintf("`%s`=%v", k, v))
				continue
			}

			err := globalVerifyObj.VerifyFieldName(k)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(v)
			if k == orm.primaryKey {
				primaryVal = val
				continue
			}
			if timeEmpty {
				val = "null"
			}

			upSet = append(upSet, fmt.Sprintf("`%s`=%s", k, val))
		}
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

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			err := globalVerifyObj.VerifyFieldName(colName)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey {
				primaryVal = val
				continue
			}
			if timeEmpty {
				continue
			}

			upSet = append(upSet, fmt.Sprintf("`%s`=%s", colName, val))
		}
	}
	if primaryVal == "" {
		return "", fmt.Errorf("sql primary where is empty, please check it")
	}

	primaryVal = strings.ReplaceAll(primaryVal, "'", "\\'")
	updateSQL = fmt.Sprintf(updateSQL, orm.tableName, util.JoinArr(upSet, ","), orm.primaryKey, primaryVal)
	return updateSQL, nil
}

func (orm *ORM) formatReplaceSQL(data interface{}) (string, error) {
	replaceSQL := "replace into `%s`(%s) values(%s)"

	if data == nil {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of insert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	var values []string

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				cols = append(cols, fmt.Sprintf("`%s`", k))
				values = append(values, fmt.Sprintf("%v", v))
				continue
			}

			err := globalVerifyObj.VerifyFieldName(k)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(v)
			if k == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				cols = append(cols, fmt.Sprintf("`%s`", k))
				values = append(values, val)
			}
		}
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

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			err := globalVerifyObj.VerifyFieldName(colName)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				cols = append(cols, fmt.Sprintf("`%s`", colName))
				values = append(values, val)
			}
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	replaceSQL = fmt.Sprintf(replaceSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","))
	return replaceSQL, nil
}

func (orm *ORM) formatReplaceManySQL(dataList []interface{}, cols []string) (string, error) {
	replaceSQL := "replace into `%s`(%s) values%s"

	if len(dataList) <= 0 {
		return "", fmt.Errorf("insert data is nil")
	}

	typeErrStr := "type of replace data is []map[string]interface{} or []*struct or []struct"

	var valArr []string
	for _, data := range dataList {
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

		subVal := "("
		for _, colName := range cols {
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal += "null,"
					continue
				}

				subVal += val + ","
			} else if v, ok = valMap["#"+colName]; ok {
				subVal += fmt.Sprintf("%v,", v)
			} else {
				subVal += "null,"
			}
		}
		subVal = subVal[:len(subVal)-1] + ")"
		valArr = append(valArr, subVal)
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("replace data is empty")
	}

	newCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		newCols = append(newCols, fmt.Sprintf("`%s`", k))
	}
	replaceSQL = fmt.Sprintf(replaceSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","))
	return replaceSQL, nil
}

func (orm *ORM) formatUpsertSQL(data interface{}) (string, error) {
	upsertSQL := "insert into `%s`(%s) values(%s) on duplicate key update %s"

	if data == nil {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"
	var cols []string
	var values []string
	var update []string

	switch data := data.(type) {
	case map[string]interface{}:
		for k, v := range data {
			if k[0] == '#' {
				k = k[1:]
				err := globalVerifyObj.VerifyFieldName(k)
				if err != nil {
					return "", err
				}
				cols = append(cols, fmt.Sprintf("`%s`", k))
				values = append(values, fmt.Sprintf("%v", v))
				update = append(update, fmt.Sprintf("`%s`=values(`%s`)", k, k))
				continue
			}

			err := globalVerifyObj.VerifyFieldName(k)
			if err != nil {
				return "", err
			}

			val, timeEmpty := formatValue(v)
			if k == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				cols = append(cols, fmt.Sprintf("`%s`", k))
				values = append(values, val)
			}
			update = append(update, fmt.Sprintf("`%s`=values(`%s`)", k, k))
		}
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

		for i := 0; i < dataValue.NumField(); i++ {
			colName := dataValue.Type().Field(i).Tag.Get("json")
			ref := dataValue.Type().Field(i).Tag.Get("ref")
			if ref != "" || colName == "" || !dataValue.Type().Field(i).IsExported() {
				continue
			}

			val, timeEmpty := formatValue(dataValue.Field(i).Interface())
			if colName == orm.primaryKey && (val == "" || val == "0") {
				continue
			}
			if timeEmpty {
				continue
			} else {
				cols = append(cols, fmt.Sprintf("`%s`", colName))
				values = append(values, val)
			}
			update = append(update, fmt.Sprintf("`%s`=values(`%s`)", colName, colName))
		}
	}
	if len(cols) <= 0 || len(values) <= 0 {
		return "", fmt.Errorf("sql data is empty, please check it")
	}

	upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(cols, ","), util.JoinArr(values, ","), util.JoinArr(update, ","))
	return upsertSQL, nil
}

func (orm *ORM) formatUpsertManySQL(dataList []interface{}, cols []string) (string, error) {
	upsertSQL := "insert into `%s`(%s) values%s on duplicate key update %s"

	if len(dataList) <= 0 {
		return "", fmt.Errorf("upsert data is nil")
	}

	typeErrStr := "type of upsert data is []map[string]interface{} or []*struct or []struct"

	var valArr []string
	for _, data := range dataList {
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

		subVal := "("
		for _, colName := range cols {
			if v, ok := valMap[colName]; ok {
				val, timeEmpty := formatValue(v)
				if (colName == orm.primaryKey && (val == "" || val == "0")) || timeEmpty {
					subVal += "null,"
					continue
				}

				subVal += val + ","
			} else if v, ok = valMap["#"+colName]; ok {
				subVal += fmt.Sprintf("%v,", v)
			} else {
				subVal += "null,"
			}
		}
		subVal = subVal[:len(subVal)-1] + ")"
		valArr = append(valArr, subVal)
	}

	if len(valArr) <= 0 {
		return "", fmt.Errorf("upsert data is empty")
	}

	newCols := make([]string, 0, len(cols))
	updateCols := make([]string, 0, len(cols))
	for _, k := range cols {
		err := globalVerifyObj.VerifyFieldName(k)
		if err != nil {
			return "", err
		}
		newCols = append(newCols, fmt.Sprintf("`%s`", k))
		updateCols = append(updateCols, fmt.Sprintf("`%s`=values(`%s`)", k, k))
	}
	upsertSQL = fmt.Sprintf(upsertSQL, orm.tableName, util.JoinArr(newCols, ","), util.JoinArr(valArr, ","), util.JoinArr(updateCols, ","))
	return upsertSQL, nil
}

func (orm *ORM) formatSaveManySQL(data interface{}) (string, error) {
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
