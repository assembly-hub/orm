package orm

import (
	"fmt"
	"testing"
)

func TestDefaultVerify_VerifyTableName(t *testing.T) {
	tableName1 := "\"wgy ` dsagh#$"
	err := globalVerifyObj.VerifyTableName(tableName1)
	if err == nil {
		t.Fail()
		return
	}
	fmt.Println(err)
}
