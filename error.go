package orm

import (
	"errors"
	"fmt"
)

var ErrClient = fmt.Errorf("db and tx are all nil")
var ErrTargetNotSettable = errors.New("[scanner]: target is not settable! a pointer is required")
