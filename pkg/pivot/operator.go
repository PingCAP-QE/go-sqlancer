package pivot

import (
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	LogicXor = Function{nil, 2, 2, "XOR", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(ConvertToBoolOrNull(a) != ConvertToBoolOrNull(b))
		return e, nil
	}}
	LogicAnd = Function{nil, 2, 2, "AND", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		boolA := ConvertToBoolOrNull(a)
		boolB := ConvertToBoolOrNull(b)
		if boolA*boolB == 0 {
			e.SetValue(false)
			return e, nil
		}
		if boolA == -1 || boolB == -1 {
			e.SetValue(nil)
			return e, nil
		}
		e.SetValue(true)
		return e, nil
	}}
	LogicOr = Function{nil, 2, 2, "OR", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		boolA := ConvertToBoolOrNull(a)
		boolB := ConvertToBoolOrNull(b)
		if boolA == 1 || boolB == 1 {
			e.SetValue(true)
			return e, nil
		}
		if boolA == -1 || boolB == -1 {
			e.SetValue(nil)
			return e, nil
		}
		e.SetValue(false)
		return e, nil
	}}
	Not = Function{nil, 1, 1, "NOT", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 1 {
			panic("error param numbers")
		}
		a := v[0]
		e := parser_driver.ValueExpr{}
		boolA := ConvertToBoolOrNull(a)
		if boolA == -1 {
			e.SetValue(nil)
			return e, nil
		}
		if boolA == 1 {
			e.SetValue(false)
			return e, nil
		}
		e.SetValue(true)
		return e, nil
	}}

	Gt = Function{nil, 2, 2, "GT", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) > 0)
		return e, nil
	}}
	Lt = Function{nil, 2, 2, "LT", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) < 0)
		return e, nil
	}}
	Ne = Function{nil, 2, 2, "NE", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) != 0)
		return e, nil
	}}
	Eq = Function{nil, 2, 2, "EQ", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) == 0)
		return e, nil
	}}
	Ge = Function{nil, 2, 2, "GE", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) >= 0)
		return e, nil
	}}
	Le = Function{nil, 2, 2, "LE", func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(compare(a, b) <= 0)
		return e, nil
	}}
)

func init() {
	for _, f := range []*Function{&LogicXor, &LogicAnd, &LogicOr, &Not} {
		f.AcceptType = make([]map[int]int, 0)
		f.AcceptType = append(f.AcceptType, *f.NewAcceptTypeMap())
		f.AcceptType = append(f.AcceptType, *f.NewAcceptTypeMap())
		// is useless for NOT op
		f.AcceptType = append(f.AcceptType, *f.NewAcceptTypeMap())
	}

	for _, f := range []*Function{&Lt, &Gt, &Le, &Ge, &Ne, &Eq} {
		f.AcceptType = make([]map[int]int, 0)
		mArg := *f.NewAcceptTypeMap()
		mArg[DatetimeArg] = AnyArg ^ StringArg ^ IntArg ^ FloatArg
		mArg[StringArg] = AnyArg ^ DatetimeArg
		mArg[IntArg] = AnyArg ^ DatetimeArg
		mArg[FloatArg] = AnyArg ^ DatetimeArg
		f.AcceptType = append(f.AcceptType, mArg, mArg)
	}
}

// -1 NULL; 0 false; 1 true
func ConvertToBoolOrNull(a parser_driver.ValueExpr) int8 {
	switch a.Kind() {
	case types.KindNull:
		return -1
	case types.KindInt64:
		if a.GetValue().(int64) != 0 {
			return 1
		}
		return 0
	case types.KindUint64:
		if a.GetValue().(uint64) != 0 {
			return 1
		}
		return 0
	case types.KindFloat32:
		if a.GetFloat32() == 0 {
			return 0
		}
		return 1
	case types.KindFloat64:
		if a.GetFloat64() == 0 {
			return 0
		}
		return 1
	case types.KindString:
		s := a.GetValue().(string)
		match, _ := regexp.MatchString(`^\-{0,1}[1-9]+|^\-{0,1}0+[1-9]`, s)
		if match {
			return 1
		}
		return 0
	case types.KindMysqlDecimal:
		d := a.GetMysqlDecimal()
		if d.IsZero() {
			return 0
		}
		return 1
	case types.KindMysqlTime:
		t := a.GetMysqlTime()
		if t.IsZero() {
			return 0
		}
		return 1
	default:
		panic(fmt.Sprintf("unreachable kind: %d", a.Kind()))
	}
}

func compare(a, b parser_driver.ValueExpr) int {
	res, _ := a.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &b.Datum)
	// NOTE: err is warning, not really error
	//fmt.Printf("@@compare a: %v t(a): %d b: %v r: %d err: %v\n", a.GetValue(), a.GetType().Tp, b.GetValue(), res, err)
	return res
}
