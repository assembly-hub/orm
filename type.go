// Package orm
package orm

import (
	"github.com/assembly-hub/db"
)

type Tx = db.Tx
type DB = db.Executor

type Where = map[string]interface{}
type Select = []string
type Order = []string
type Limit = []uint
type GroupBy = []string
type Having = map[string]interface{}
