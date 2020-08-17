package executor

import (
	"bytes"
	"time"

	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/ngaut/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

// BufferOut parser ast node to SQL string
func BufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return string(out.Bytes()), nil
}

// TimeMustParse wrap time.Parse and panic when error
func TimeMustParse(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		log.Fatalf("parse time err %+v, layout: %s, value: %s", err, layout, value)
	}
	return t
}

// RdType rand data type
func RdType() string {
	switch util.Rd(6) {
	case 0:
		return "varchar"
	case 1:
		return "text"
	case 2:
		return "timestamp"
	case 3:
		return "datetime"
	}
	return "int"
}

// RdDataLen rand data with given type
func RdDataLen(t string) int {
	switch t {
	case "int":
		return int(util.RdRange(1, 20))
	case "varchar":
		return int(util.RdRange(1, 2047))
	case "float":
		return int(util.RdRange(16, 64))
	case "timestamp":
		return -1
	case "datetime":
		return -1
	case "text":
		return -1
	}
	return 10
}

// RdColumnOptions for rand column option with given type
func RdColumnOptions(t string) (options []ast.ColumnOptionType) {
	if util.Rd(3) == 0 {
		options = append(options, ast.ColumnOptionNotNull)
	} else if util.Rd(2) == 0 {
		options = append(options, ast.ColumnOptionNull)
	}
	switch t {
	case "varchar", "timestamp", "datetime", "int":
		if util.Rd(2) == 0 {
			options = append(options, ast.ColumnOptionDefaultValue)
		}
	}
	return
}

// RdCharset rand charset
func RdCharset() string {
	switch util.Rd(4) {
	default:
		return "utf8"
	}
}

// RdBool ...
func RdBool() bool {
	return util.Rd(2) == 0
}

func DataType2Len(t string) int {
	switch t {
	case "int":
		return 16
	case "bigint":
		return 64
	case "varchar":
		return 511
	case "timestamp":
		return 255
	case "datetime":
		return 255
	case "text":
		return 511
	case "float":
		return 64
	}
	return 16
}
