// Package orm
package orm

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/assembly-hub/basics/set"
	"github.com/assembly-hub/basics/util"
	"github.com/assembly-hub/db"
	"github.com/assembly-hub/log"
	"github.com/assembly-hub/log/empty"
	"github.com/assembly-hub/task/execute"

	"github.com/assembly-hub/orm/dbtype"
)

const (
	selectColLinkStr  = "__"
	defaultPrimaryKey = "id"
	defaultBatchSize  = 200
	defaultLimitMax   = 50
)

var (
	notReadyLastInsertIDSet set.Set[int]
)

func init() {
	notReadyLastInsertIDSet = set.New[int]()
	notReadyLastInsertIDSet.Add(dbtype.SQLServer, dbtype.Postgres, dbtype.OpenGauss)
}

// InitNotReadyLastInsertID 初始化方法 LastInsertID 没有实现的数据库类型，会覆盖默认设置
func InitNotReadyLastInsertID(db ...int) {
	notReadyLastInsertIDSet.Clear()
	notReadyLastInsertIDSet.Add(db...)
}

type databaseQuery struct {
	Distinct        bool
	SelectForUpdate bool
	Select          []string
	Order           []string
	Limit           []uint
	Where           map[string]interface{}
	GroupBy         []string
	Having          map[string]interface{}
}

func newDBQuery() *databaseQuery {
	q := new(databaseQuery)
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

// ORM
// # 为内置符号，标志为原始字段，不进行任何处理，仅在以下数据有效：
// Select Order GroupBy Where Having
type ORM struct {
	// 用户自定义SQL，优先级最高
	customSQL string
	tableName string
	executor  db.Executor
	tx        db.Tx
	ref       *Reference

	ctx context.Context

	// 主键
	primaryKey string

	// 唯一键列表
	uniqueKeys set.Set[string]

	// sql server 排除主键
	sqlserverExcludePK bool

	// oracle merge union 类型，默认是 union all
	oracleMergeUnionAll bool

	// 字段别名链接字符串
	selectColLinkStr string

	// 保留上次查询参数
	keepQuery bool

	// 默认limit
	limit uint

	// 查询配置数据
	Q      *databaseQuery
	logger log.Log
}

type Paging struct {
	PageNo    int `json:"page_no"`    //当前页
	PageSize  int `json:"page_size"`  //每页条数
	Total     int `json:"total"`      //总条数
	PageTotal int `json:"page_total"` //总页数
}

func NewORM(ctx context.Context, tableName string, executor db.Executor, ref *Reference) *ORM {
	err := globalVerifyObj.VerifyTableName(tableName)
	if err != nil {
		panic(err)
	}

	if ref == nil {
		panic("database reference is nil")
	}

	if executor == nil {
		panic("executor is nil")
	}

	dao := initORM()
	dao.tableName = tableName
	dao.executor = executor
	dao.ref = ref
	dao.ctx = ctx
	return dao
}

func NewORMWithTx(ctx context.Context, tableName string, tx db.Tx, ref *Reference) *ORM {
	err := globalVerifyObj.VerifyTableName(tableName)
	if err != nil {
		panic(err)
	}

	if ref == nil {
		panic("database reference is nil")
	}

	if tx == nil {
		panic("tx is nil")
	}

	dao := initORM()
	dao.tableName = tableName
	dao.tx = tx
	dao.ref = ref
	dao.ctx = ctx
	return dao
}

func initORM() *ORM {
	dao := new(ORM)
	dao.uniqueKeys = set.New[string]()
	dao.sqlserverExcludePK = true
	dao.oracleMergeUnionAll = true
	dao.keepQuery = true
	dao.selectColLinkStr = "_"
	dao.Q = newDBQuery()
	dao.primaryKey = defaultPrimaryKey
	dao.limit = 0
	dao.logger = empty.NoLog
	return dao
}

// GetExecutor 获取原始的数据库链接
func (orm *ORM) GetExecutor() db.Executor {
	return orm.executor
}

// GetTx 获取原始的Tx链接
func (orm *ORM) GetTx() db.Tx {
	return orm.tx
}

func (orm *ORM) Logger(logger log.Log) {
	if logger != nil {
		orm.logger = logger
	}
}

// GetBaseExecutor 获取原始的数据库链接或Tx
// conn 按需断言
func (orm *ORM) GetBaseExecutor() (conn db.BaseExecutor, isTx bool) {
	if orm.tx != nil {
		return orm.tx, true
	}
	return orm.executor, false
}

// SetDefLimit 修改默认limit
func (orm *ORM) SetDefLimit(n uint) *ORM {
	orm.limit = n
	return orm
}

// SQLServerExcludePK sql server 排除主键
func (orm *ORM) SQLServerExcludePK(exclude bool) *ORM {
	orm.sqlserverExcludePK = exclude
	return orm
}

// OracleMergeUnionAll oracle merge union 类型，默认是 true
func (orm *ORM) OracleMergeUnionAll(all bool) *ORM {
	orm.oracleMergeUnionAll = all
	return orm
}

// UniqueKeys 设置新的唯一键，用于 upsert
func (orm *ORM) UniqueKeys(uniqueKey ...string) *ORM {
	if len(uniqueKey) <= 0 {
		orm.uniqueKeys.Clear()
		return orm
	}

	orm.uniqueKeys.Add(uniqueKey...)
	return orm
}

func (orm *ORM) SelectColLinkStr(s string) *ORM {
	orm.selectColLinkStr = s
	return orm
}

// CustomSQL 设置自定义SQL，可以为空，为空表示移除自定义sql
func (orm *ORM) CustomSQL(sql string) *ORM {
	orm.customSQL = sql
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
			orm.Q.Where[util.Any2String(pair[i*2])] = v.cond(false)
		default:
			orm.Q.Where[util.Any2String(pair[i*2])] = v
		}
	}
	return orm
}

func (orm *ORM) Clone(ctx context.Context) *ORM {
	dao := new(ORM)
	dao.tableName = orm.tableName
	dao.executor = orm.executor
	dao.tx = orm.tx
	dao.ref = orm.ref
	dao.selectColLinkStr = orm.selectColLinkStr
	dao.Q = newDBQuery()
	dao.ctx = ctx
	dao.primaryKey = orm.primaryKey
	dao.logger = orm.logger
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
	orm.Q = newDBQuery()
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
		CustomSQL:        orm.customSQL,
		PrivateKey:       orm.primaryKey,
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
	if orm.customSQL != "" {
		return orm.customSQL
	}

	q := BaseQuery{
		CustomSQL:        orm.customSQL,
		PrivateKey:       orm.primaryKey,
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

func (orm *ORM) PageData(result interface{}, flat bool, pageNo, pageSize uint) (pg *Paging, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if orm.customSQL != "" {
		return nil, ErrCustomSQL
	}

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

func (orm *ORM) ToData(result interface{}, flat bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if !orm.keepQuery {
		defer func() {
			orm.ClearCache()
		}()
	}

	q := BaseQuery{
		CustomSQL:        orm.customSQL,
		PrivateKey:       orm.primaryKey,
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
	if len(q.Limit) <= 0 && orm.limit > 0 {
		q.Limit = []uint{orm.limit}
	}
	if !flat {
		q.SelectColLinkStr = selectColLinkStr
	}

	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	return toData(orm.ctx, sqlDB, &q, result, flat)
}

func (orm *ORM) FetchData(dataType interface{}, flat bool, fetch func(row interface{}) bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if !orm.keepQuery {
		defer func() {
			orm.ClearCache()
		}()
	}

	q := BaseQuery{
		CustomSQL:        orm.customSQL,
		PrivateKey:       orm.primaryKey,
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
	if len(q.Limit) <= 0 && orm.limit > 0 {
		q.Limit = []uint{orm.limit}
	}
	if !flat {
		q.SelectColLinkStr = selectColLinkStr
	}

	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}
	return fetchData(orm.ctx, sqlDB, &q, dataType, flat, fetch)
}

func (orm *ORM) ExecuteSQL(customSQL string) (affectedRow int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	var ret sql.Result
	if orm.tx != nil {
		ret, err = orm.tx.ExecContext(orm.ctx, customSQL)
	} else if orm.executor != nil {
		ret, err = orm.executor.ExecContext(orm.ctx, customSQL)
	} else {
		return 0, ErrClient
	}
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

// Exist 检查数据是否存在
func (orm *ORM) Exist() (b bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if orm.customSQL != "" {
		return false, ErrCustomSQL
	}

	if !orm.keepQuery {
		defer func() {
			orm.ClearCache()
		}()
	}

	q := BaseQuery{
		PrivateKey:       orm.primaryKey,
		RefConf:          orm.ref,
		TableName:        orm.tableName,
		Where:            orm.Q.Where,
		SelectColLinkStr: orm.selectColLinkStr,
		Order:            orm.Q.Order,
		Distinct:         orm.Q.Distinct,
		SelectForUpdate:  orm.Q.SelectForUpdate,
		Limit:            Limit{1},
		Select:           Select{orm.primaryKey},
		GroupBy:          orm.Q.GroupBy,
		Having:           orm.Q.Having,
	}

	var c int64
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}
	err = toData(orm.ctx, sqlDB, &q, &c, false)
	if err != nil {
		return false, err
	}

	if c > 0 {
		return true, nil
	}
	return false, nil
}

func (orm *ORM) Count(clearCache bool) (c int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if orm.customSQL != "" {
		return 0, ErrCustomSQL
	}

	if clearCache {
		defer func() {
			orm.ClearCache()
		}()
	}
	q := BaseQuery{
		CustomSQL:        orm.customSQL,
		PrivateKey:       orm.primaryKey,
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

	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}
	return count(orm.ctx, sqlDB, &q)
}

func (orm *ORM) InsertOne(data interface{}) (insertID int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	insertSQL, err := orm.formatInsertSQL(data)
	if err != nil {
		return 0, err
	}

	var ret sql.Result
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	if sqlDB != nil {
		ret, err = sqlDB.ExecContext(orm.ctx, insertSQL)
	} else {
		return 0, ErrClient
	}
	if err != nil {
		return 0, err
	}

	if notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
		return -1, nil
	}

	return ret.LastInsertId()
}

// InsertMany 每条数据字段不一致或者想获取每条数据的主键，使用此方法
func (orm *ORM) InsertMany(data []interface{}, trans bool) (affected int64, insertIDs []int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

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

	if trans && orm.executor != nil {
		tx, errTx := orm.executor.BeginTx(orm.ctx, nil)
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

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected

			if !notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
				insertID, err := execContext.LastInsertId()
				if err != nil {
					panic(err)
				}
				insertIDs = append(insertIDs, insertID)
			}
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
			} else if orm.executor != nil {
				execContext, err = orm.executor.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, nil, ErrClient
			}

			if err != nil {
				panic(err)
			}

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected

			if !notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
				insertID, err := execContext.LastInsertId()
				if err != nil {
					panic(err)
				}
				insertIDs = append(insertIDs, insertID)
			}
		}
	}
	return
}

// InsertManySameClos 字段一致使用此方法
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) InsertManySameClos(data []interface{}, cols []string, batchSize int, trans bool) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	if len(cols) <= 0 {
		return 0, fmt.Errorf("no cols")
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	dataLen := len(data)
	sqlTask := execute.NewExecutor("sql task")
	sqlTask.Logger(orm.logger)

	for i := 0; i < dataLen; i += batchSize {
		ends := i + batchSize
		if ends > dataLen {
			ends = dataLen
		}

		sqlTask.AddFlexible(orm.formatInsertManySQL, data[i:ends], cols)
	}

	sqlArr, taskErrList, err := sqlTask.ExecuteWithErr(orm.ctx)
	if err != nil {
		return 0, err
	}

	for _, e := range taskErrList {
		if e != nil {
			return 0, e
		}
	}

	affected, err = orm.executeSQL(sqlArr, trans)
	return
}

func (orm *ORM) UpsertOne(data interface{}) (insertID int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	insertSQL, err := orm.formatUpsertSQL(data)
	if err != nil {
		return 0, err
	}

	var ret sql.Result
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	if sqlDB != nil {
		ret, err = sqlDB.ExecContext(orm.ctx, insertSQL)
	} else {
		return 0, ErrClient
	}
	if err != nil {
		return 0, err
	}

	if notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
		return -1, nil
	}

	return ret.LastInsertId()
}

// UpsertMany 每条数据字段不一致或者想获取每条数据的主键，使用此方法
func (orm *ORM) UpsertMany(data []interface{}, trans bool) (affected int64, insertIDs []int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

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

	if trans && orm.executor != nil {
		tx, errTx := orm.executor.BeginTx(orm.ctx, nil)
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

			rowsAffected, err := execContext.RowsAffected()
			if err != nil {
				panic(err)
			}
			affected += rowsAffected

			if !notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
				insertID, err := execContext.LastInsertId()
				if err != nil {
					panic(err)
				}
				insertIDs = append(insertIDs, insertID)
			}
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
			} else if orm.executor != nil {
				execContext, err = orm.executor.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, nil, ErrClient
			}
			if err != nil {
				panic(err)
			}

			if !notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
				insertID, err := execContext.LastInsertId()
				if err != nil {
					panic(err)
				}
				insertIDs = append(insertIDs, insertID)
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

// UpsertManySameClos
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) UpsertManySameClos(data []interface{}, cols []string, batchSize int, trans bool) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	if len(cols) <= 0 {
		return 0, fmt.Errorf("no cols")
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	dataLen := len(data)
	sqlTask := execute.NewExecutor("sql task")
	sqlTask.Logger(orm.logger)
	for i := 0; i < dataLen; i += batchSize {
		ends := i + batchSize
		if ends > dataLen {
			ends = dataLen
		}

		sqlTask.AddFlexible(orm.formatUpsertManySQL, data[i:ends], cols)
	}

	sqlArr, taskErrList, err := sqlTask.ExecuteWithErr(orm.ctx)
	if err != nil {
		return 0, err
	}

	for _, e := range taskErrList {
		if e != nil {
			return 0, e
		}
	}

	affected, err = orm.executeSQL(sqlArr, trans)
	return
}

// SaveMany
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) SaveMany(data []interface{}, trans bool) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	var sqlArr []string

	for _, d := range data {
		upsertSQL, err := orm.formatSaveSQL(d)
		if err != nil {
			return 0, err
		}

		sqlArr = append(sqlArr, upsertSQL)
	}

	if trans && orm.executor != nil {
		tx, errTx := orm.executor.BeginTx(orm.ctx, nil)
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
			} else if orm.executor != nil {
				execContext, err = orm.executor.ExecContext(orm.ctx, sqlObj)
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
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	affected, err = 0, nil

	dbCore := orm.ref.getDBConf()

	updateSQL := "update " + dbCore.EscStart + "%s" + dbCore.EscEnd + " set %s"

	if update == nil {
		return 0, fmt.Errorf("update data is nil")
	}

	if len(where) > 0 {
		q := BaseQuery{
			PrivateKey: orm.primaryKey,
			RefConf:    orm.ref,
			TableName:  orm.tableName,
			Where:      where,
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
			value, timeEmpty := orm.formatValue(v)
			if timeEmpty {
				val = "null"
			} else {
				val = value
			}
		}

		updateSet = append(updateSet, fmt.Sprintf("%s%s%s=%s",
			dbCore.EscStart, k, dbCore.EscEnd, val))
	}

	updateSQL = fmt.Sprintf(updateSQL, orm.tableName, util.JoinArr(updateSet, ","))

	var ret sql.Result
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	if sqlDB != nil {
		ret, err = sqlDB.ExecContext(orm.ctx, updateSQL)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}

	return ret.RowsAffected()
}

// UpdateMany 主键，id 不能为空，为空将更新失败
func (orm *ORM) UpdateMany(data []interface{}, trans bool) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

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

	if trans && orm.executor != nil {
		tx, errTx := orm.executor.BeginTx(orm.ctx, nil)
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
			} else if orm.executor != nil {
				execContext, err = orm.executor.ExecContext(orm.ctx, sqlObj)
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

// UpdateOne 主键，id 不能为空，为空将更新失败
func (orm *ORM) UpdateOne(data interface{}) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	updateSQL, err := orm.formatUpdateSQL(data)
	if err != nil {
		return 0, err
	}

	var execContext sql.Result
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	if sqlDB != nil {
		execContext, err = sqlDB.ExecContext(orm.ctx, updateSQL)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}

	return execContext.RowsAffected()
}

func (orm *ORM) ReplaceOne(data interface{}) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	replaceSQL, err := orm.formatReplaceSQL(data)
	if err != nil {
		return 0, err
	}

	var execContext sql.Result
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	if sqlDB != nil {
		execContext, err = sqlDB.ExecContext(orm.ctx, replaceSQL)
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}
	if notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
		return -1, nil
	}
	return execContext.LastInsertId()
}

// ReplaceMany 每条数据字段不一致或者想获取每条数据的主键，使用此方法
func (orm *ORM) ReplaceMany(data []interface{}, trans bool) (affected int64, insertIds []int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

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

	if trans && orm.executor != nil {
		tx, errTx := orm.executor.BeginTx(orm.ctx, nil)
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

			if !notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
				insertID, err := execContext.LastInsertId()
				if err != nil {
					panic(err)
				}
				insertIds = append(insertIds, insertID)
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
			} else if orm.executor != nil {
				execContext, err = orm.executor.ExecContext(orm.ctx, sqlObj)
			} else {
				return 0, nil, ErrClient
			}
			if err != nil {
				panic(err)
			}

			if !notReadyLastInsertIDSet.Has(orm.ref.dbConf.DBType) {
				insertID, err := execContext.LastInsertId()
				if err != nil {
					panic(err)
				}
				insertIds = append(insertIds, insertID)
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

// ReplaceManySameClos
// data 需要处理的数据集合，数据格式为：map、*struct、struct，字段不在 cols 的被赋值null
// cols 需要处理的字段集合
// batchSize 单次并发数量
// trans 是否使用事务
func (orm *ORM) ReplaceManySameClos(data []interface{}, cols []string, batchSize int, trans bool) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	if len(data) <= 0 {
		return 0, fmt.Errorf("no data")
	}

	if len(cols) <= 0 {
		return 0, fmt.Errorf("no cols")
	}

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	dataLen := len(data)
	sqlTask := execute.NewExecutor("sql task")
	sqlTask.Logger(orm.logger)

	for i := 0; i < dataLen; i += batchSize {
		ends := i + batchSize
		if ends > dataLen {
			ends = dataLen
		}

		sqlTask.AddFlexible(orm.formatReplaceManySQL, data[i:ends], cols)
	}

	sqlArr, taskErrList, err := sqlTask.ExecuteWithErr(orm.ctx)
	if err != nil {
		return 0, err
	}

	for _, e := range taskErrList {
		if e != nil {
			return 0, e
		}
	}

	affected, err = orm.executeSQL(sqlArr, trans)
	return
}

func (orm *ORM) DeleteByWhere(where map[string]interface{}) (affected int64, err error) {
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%v", p)
			}
		}
	}()

	affected, err = 0, nil

	dbCore := orm.ref.getDBConf()
	var delSQL strings.Builder
	delSQL.Grow(100)
	delSQL.WriteString("delete from ")
	delSQL.WriteString(dbCore.EscStart)
	delSQL.WriteString(orm.tableName)
	delSQL.WriteString(dbCore.EscEnd)

	if len(where) > 0 {
		q := BaseQuery{
			PrivateKey: orm.primaryKey,
			RefConf:    orm.ref,
			TableName:  orm.tableName,
			Where:      where,
		}
		delSQL.WriteString(" where ")
		delSQL.WriteString(q.GetWhere())
	}

	var ret sql.Result
	var sqlDB db.BaseExecutor = orm.tx
	if sqlDB == nil {
		sqlDB = orm.executor
	}

	if sqlDB != nil {
		ret, err = sqlDB.ExecContext(orm.ctx, delSQL.String())
	} else {
		return 0, ErrClient
	}

	if err != nil {
		return 0, err
	}

	return ret.RowsAffected()
}
