// package orm
package orm

import (
	"database/sql"
)

type Tx = sql.Tx
type DB = sql.DB

type Where = map[string]interface{}
type Select = []string
type Order = []string
type Limit = []uint
type GroupBy = []string
type Having = map[string]interface{}
