// package orm
package orm

import "strings"

type joinType string

func (s joinType) isNatural() bool {
	i := strings.Index(strings.ToLower(string(s)), "natural")
	return i >= 0
}

func (s joinType) value() string {
	return string(s)
}

func toJoinData(str string) joinType {
	str = strings.ToLower(str)
	switch str {
	case "":
		return defaultJoinType
	case "left":
		return leftJoinType
	case "right":
		return rightJoinType
	case "inner":
		return innerJoinType
	case "natural left":
		return naturalLeftJoinType
	case "natural right":
		return naturalRightJoinType
	case "natural inner":
		return naturalInnerJoinType
	default:
		panic("join type error")
	}
}

const (
	defaultJoinType      = joinType("")
	leftJoinType         = joinType("left")
	rightJoinType        = joinType("right")
	innerJoinType        = joinType("inner")
	naturalLeftJoinType  = joinType("natural left")
	naturalRightJoinType = joinType("natural right")
	naturalInnerJoinType = joinType("natural inner")
)
