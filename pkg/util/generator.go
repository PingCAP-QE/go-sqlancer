package util

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	. "github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	parser_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

// -1 NULL; 0 false; 1 true
func ConvertToBoolOrNull(a parser_driver.ValueExpr) int8 {
	switch a.Kind() {
	case tidb_types.KindNull:
		return -1
	case tidb_types.KindInt64:
		if a.GetValue().(int64) != 0 {
			return 1
		}
		return 0
	case tidb_types.KindUint64:
		if a.GetValue().(uint64) != 0 {
			return 1
		}
		return 0
	case tidb_types.KindFloat32:
		if a.GetFloat32() == 0 {
			return 0
		}
		return 1
	case tidb_types.KindFloat64:
		if a.GetFloat64() == 0 {
			return 0
		}
		return 1
	case tidb_types.KindString:
		s := a.GetValue().(string)
		re, _ := regexp.Compile(`^[-+]?[0-9]*\.?[0-9]+`)
		matchall := re.FindAllString(s, -1)
		if len(matchall) == 0 {
			return 0
		}
		numStr := matchall[0]
		match, _ := regexp.MatchString(`^[-+]?0*\.?0+$`, numStr)
		if match {
			return 0
		}
		return 1
	case tidb_types.KindMysqlDecimal:
		d := a.GetMysqlDecimal()
		if d.IsZero() {
			return 0
		}
		return 1
	case tidb_types.KindMysqlTime:
		t := a.GetMysqlTime()
		if t.IsZero() {
			return 0
		}
		return 1
	default:
		panic(fmt.Sprintf("unreachable kind: %d", a.Kind()))
	}
}

func Compare(a, b parser_driver.ValueExpr) int {
	res, _ := a.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &b.Datum)
	// NOTE: err is warning, not really error
	//fmt.Printf("@@compare a: %v t(a): %d b: %v r: %d err: %v\n", a.GetValue(), a.GetType().Tp, b.GetValue(), res, err)
	return res
}

func BufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

// TODO: decimal NOT Equals to float
func TransMysqlType(t *parser_types.FieldType) int {
	switch t.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return IntArg
	case mysql.TypeDecimal, mysql.TypeFloat, mysql.TypeDouble:
		return FloatArg
	case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
		return DatetimeArg
	case mysql.TypeVarchar, mysql.TypeJSON, mysql.TypeVarString, mysql.TypeString:
		return StringArg
	case mysql.TypeNull:
		return NullArg
	default:
		panic(fmt.Sprintf("no implement for type: %s", t.String()))
	}
}

// TODO: decimal NOT Equals to float
func TransStringType(s string) int {
	s = strings.ToLower(s)
	switch {
	case strings.Contains(s, "string"), strings.Contains(s, "char"), strings.Contains(s, "text"), strings.Contains(s, "json"):
		return StringArg
	case strings.Contains(s, "int"), strings.Contains(s, "long"), strings.Contains(s, "short"), strings.Contains(s, "tiny"):
		return IntArg
	case strings.Contains(s, "float"), strings.Contains(s, "decimal"), strings.Contains(s, "double"):
		return FloatArg
	case strings.Contains(s, "time"), strings.Contains(s, "date"):
		return DatetimeArg
	default:
		panic(fmt.Sprintf("no implement for type: %s", s))
	}
}

func TransToMysqlType(i int) byte {
	switch i {
	case IntArg:
		return mysql.TypeLong
	case FloatArg:
		return mysql.TypeDouble
	case DatetimeArg:
		return mysql.TypeDatetime
	case StringArg:
		return mysql.TypeVarchar
	default:
		panic(fmt.Sprintf("no implement this type: %d", i))
	}
}

func CompareValue(a, b parser_driver.ValueExpr) bool {
	if a.Type.Tp != b.Type.Tp {
		return false
	}
	if a.Datum.GetValue() != b.Datum.GetValue() {
		return false
	}
	return true
}
