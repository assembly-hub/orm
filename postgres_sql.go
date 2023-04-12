package orm

import (
	"strings"
)

func (p *queryModel) postgresIgnoreFormatSubSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string,
	colData interface{}) string {
	var subSQL strings.Builder
	subSQL.Grow(20)

	subSQL.WriteString("LOWER(")
	subSQL.WriteString(colName)
	subSQL.WriteByte(')')
	switch colOperator {
	case "eq":
		subSQL.WriteString("=LOWER(")
	case "lt":
		subSQL.WriteString("<LOWER(")
	case "lte":
		subSQL.WriteString("<=LOWER(")
	case "gt":
		subSQL.WriteString(">LOWER(")
	case "gte":
		subSQL.WriteString(">=LOWER(")
	case "ne":
		subSQL.WriteString("<>LOWER(")
	case "in":
		val = connectStrArr(rawStrArr, ",", "LOWER('", "')")
		subSQL.WriteString(" in (")
	case "nin":
		val = connectStrArr(rawStrArr, ",", "LOWER('", "')")
		subSQL.WriteString(" not in (")
	default:
		return p.postgresIgnoreLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	subSQL.WriteString(val)
	subSQL.WriteByte(')')
	return subSQL.String()
}

func (p *queryModel) postgresIgnoreLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "startswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		}
	case "endswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		}
	case "contains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" ilike '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "customlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" and ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" ilike '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		return p.postgresIgnoreOrLikeSQL(colOperator, colName, val, rawVal, rawStrArr)
	}
	return subSQL.String()
}

func (p *queryModel) postgresIgnoreOrLikeSQL(colOperator string, colName string, val, rawVal string, rawStrArr []string) string {
	var subSQL strings.Builder
	subSQL.Grow(20)
	switch colOperator {
	case "orstartswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" ilike '")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orendswith":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" ilike '%")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	case "orcontains":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '%")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("%'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" ilike '%")
				subSQL.WriteString(v)
				subSQL.WriteString("%'")
			}
			subSQL.WriteByte(')')
		}
	case "orcustomlike":
		if rawVal != "" {
			subSQL.WriteString(colName)
			subSQL.WriteString(" ilike '")
			subSQL.WriteString(rawVal)
			subSQL.WriteString("'")
		} else if len(rawStrArr) > 0 {
			for _, v := range rawStrArr {
				if subSQL.Len() <= 0 {
					subSQL.WriteByte('(')
				} else {
					subSQL.WriteString(" or ")
				}
				subSQL.WriteString(colName)
				subSQL.WriteString(" ilike '")
				subSQL.WriteString(v)
				subSQL.WriteString("'")
			}
			subSQL.WriteByte(')')
		}
	default:
		panic("no definition")
	}
	return subSQL.String()
}
