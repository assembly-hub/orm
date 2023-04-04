// package orm
package orm

import "strings"

type joinType string

func (s joinType) value() string {
	return string(s)
}

func toJoinData(str string) joinType {
	str = strings.ToLower(str)
	return joinType(str)
}
