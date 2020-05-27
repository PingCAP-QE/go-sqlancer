package util

import (
	"bytes"
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	parser_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var OpFuncGroupByRet types.OpFuncIndex = make(types.OpFuncIndex)

func RegisterToOpFnIndex(o types.OpFuncEval) {
	returnType := o.GetPossibleReturnType()

	for returnType != 0 {
		// i must be n-th power of 2: 1001100 => i(0000100)
		i := returnType &^ (returnType - 1)
		if _, ok := OpFuncGroupByRet[i]; !ok {
			OpFuncGroupByRet[i] = make(map[string]types.OpFuncEval)
		}
		if _, ok := OpFuncGroupByRet[i][o.GetName()]; !ok {
			OpFuncGroupByRet[i][o.GetName()] = o
		}
		// make the last non-zero bit to be zero: 1001100 => 1001000
		returnType &= (returnType - 1)
	}
}

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

// TODO(mahjonp) because of CompareDatum can tell us whether there exists an truncate, we can use it in `comparisionValidator`
func Compare(a, b parser_driver.ValueExpr) int {
	res, _ := a.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &b.Datum)
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
func TransMysqlType(t *parser_types.FieldType) uint64 {
	switch t.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		return TypeIntArg
	case mysql.TypeDecimal, mysql.TypeFloat, mysql.TypeDouble:
		return TypeFloatArg
	case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
		return TypeDatetimeArg
	case mysql.TypeVarchar, mysql.TypeJSON, mysql.TypeVarString, mysql.TypeString:
		return TypeStringArg
		// Note: Null is base of all types
	case mysql.TypeNull:
		arr := []uint64{TypeIntArg, TypeFloatArg, TypeDatetimeArg, TypeStringArg}
		return arr[rand.Intn(len(arr))]
	default:
		panic(fmt.Sprintf("no implement for type: %s", t.String()))
	}
}

// TODO: decimal NOT Equals to float
func TransStringType(s string) uint64 {
	s = strings.ToLower(s)
	switch {
	case strings.Contains(s, "string"), strings.Contains(s, "char"), strings.Contains(s, "text"), strings.Contains(s, "json"):
		return TypeStringArg
	case strings.Contains(s, "int"), strings.Contains(s, "long"), strings.Contains(s, "short"), strings.Contains(s, "tiny"):
		return TypeIntArg
	case strings.Contains(s, "float"), strings.Contains(s, "decimal"), strings.Contains(s, "double"):
		return TypeFloatArg
	case strings.Contains(s, "time"), strings.Contains(s, "date"):
		return TypeDatetimeArg
	default:
		panic(fmt.Sprintf("no implement for type: %s", s))
	}
}

func TransToMysqlType(i uint64) byte {
	switch i {
	case TypeIntArg:
		return mysql.TypeLong
	case TypeFloatArg:
		return mysql.TypeDouble
	case TypeDatetimeArg:
		return mysql.TypeDatetime
	case TypeStringArg:
		return mysql.TypeVarchar
	default:
		panic(fmt.Sprintf("no implement this type: %d", i))
	}
}

// CompareValue is only used in test cases
func CompareValue(a, b parser_driver.ValueExpr) bool {
	if a.Type.Tp != b.Type.Tp {
		return false
	}
	if a.Datum.GetValue() != b.Datum.GetValue() {
		return false
	}
	return true
}
