package orm

import (
	"errors"
	"fmt"
)

var ErrClient = fmt.Errorf("db and tx are all nil")
var ErrTargetNotSettable = errors.New("[scanner]: target is not settable! a pointer is required")
var ErrBetweenValueMatch = errors.New("[between]: the parameter array length is required to be 2")
var ErrCustomSQL = errors.New("custom sql does not allow this operation")
var ErrDBType = errors.New("the current database type is not currently supported")
var ErrDBFunc = errors.New("this method is not currently supported in the current database")
var ErrTooManyColumn = errors.New("too many columns")
var ErrTooFewColumn = errors.New("too few columns")
var ErrMapKeyType = errors.New("map's key type must be \"String\"")
var ErrParams = errors.New("when \"flat=false\", the value of the map can only be interface{}")
