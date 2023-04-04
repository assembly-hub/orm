// package orm
package orm

import "fmt"

var globalVerifyObj Verify

func init() {
	globalVerifyObj = newDefaultVerify()
}

func SetGlobalVerify(v Verify) {
	if v == nil {
		panic("verify cannot nil")
	}
	globalVerifyObj = v
}

type Verify interface {
	VerifyTableName(name string) error
	VerifyFieldName(name string) error
	VerifyTagName(name string) error
}

type defaultVerify struct {
}

func (d *defaultVerify) VerifyTagName(name string) error {
	if len(name) > 64 {
		return fmt.Errorf("the tag name[%s] is too long. the maximum requirement is 64", name)
	}

	for _, ch := range name {
		if !(ch == '_' || ch == '-' || (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z')) {
			return fmt.Errorf("tag name[%s] cannot have \"%c\", "+
				"customize verify as needed to achieve interface \"Verify\"", name, ch)
		}
	}

	return nil
}

func (d *defaultVerify) VerifyTableName(name string) error {
	if len(name) > 64 {
		return fmt.Errorf("the table name[%s] is too long. the maximum requirement is 64", name)
	}

	for _, ch := range name {
		if !(ch == '_' || ch == '-' || (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z')) {
			return fmt.Errorf("table name[%s] cannot have \"%c\", "+
				"customize verify as needed to achieve interface \"Verify\"", name, ch)
		}
	}

	return nil
}

func (d *defaultVerify) VerifyFieldName(name string) error {
	if len(name) > 64 {
		return fmt.Errorf("the field name[%s] is too long. the maximum requirement is 64", name)
	}

	for _, ch := range name {
		if !(ch == '_' || ch == '-' || (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z')) {
			return fmt.Errorf("field name[%s] cannot have \"%c\", "+
				"customize verify as needed to achieve interface \"Verify\"", name, ch)
		}
	}

	return nil
}

func newDefaultVerify() Verify {
	v := new(defaultVerify)
	return v
}
