package executor

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"

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

// MinInt ...
func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// MaxInt ...
func MaxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// Rd same to rand.Intn
func Rd(n int) int {
	return rand.Intn(n)
}

// RdRange rand int in range
func RdRange(n, m int) int {
	if n == m {
		return n
	}
	if m < n {
		n, m = m, n
	}
	return n + rand.Intn(m-n)
}

// RdFloat64 rand float64
func RdFloat64() float64 {
	return rand.Float64()
}

// RdDate rand date
func RdDate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 1, 0, time.UTC).Unix()
	max := time.Date(2100, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdTimestamp return same format as RdDate except rand range
// TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07'
func RdTimestamp() time.Time {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdString rand string with given length
func RdString(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(33, 127)
		// char '\' and '"' should be escaped
		if charCode == 92 || charCode == 34 {
			charCode++
			// res = fmt.Sprintf("%s%s", res, "\\")
		}
		res = fmt.Sprintf("%s%s", res, string(rune(charCode)))
	}
	return res
}

// RdStringChar rand string with given length, letter chars only
func RdStringChar(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(97, 123)
		res = fmt.Sprintf("%s%s", res, string(rune(charCode)))
	}
	return res
}

// RdType rand data type
func RdType() string {
	switch Rd(6) {
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
		return RdRange(1, 20)
	case "varchar":
		return RdRange(1, 2047)
	case "float":
		return RdRange(16, 64)
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
	if Rd(3) == 0 {
		options = append(options, ast.ColumnOptionNotNull)
	} else if Rd(2) == 0 {
		options = append(options, ast.ColumnOptionNull)
	}
	switch t {
	case "varchar", "timestamp", "datetime", "int":
		if Rd(2) == 0 {
			options = append(options, ast.ColumnOptionDefaultValue)
		}
	}
	return
}

// RdCharset rand charset
func RdCharset() string {
	switch Rd(4) {
	default:
		return "utf8"
	}
}

// RdBool ...
func RdBool() bool {
	return Rd(2) == 0
}

func dataType2Len(t string) int {
	switch t {
	case "int":
		return 16
	case "varchar":
		return 1023
	case "timestamp":
		return 255
	case "datetime":
		return 255
	case "text":
		return 1023
	case "float":
		return 64
	}
	return 16
}
