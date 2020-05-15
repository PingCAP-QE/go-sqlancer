package operator

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/opcode"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	Gt = types.NewOp(opcode.GT, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(Compare(a, b) > 0)
		return e, nil
	})

	Lt = types.NewOp(opcode.LT, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(Compare(a, b) < 0)
		return e, nil
	})

	Ne = types.NewOp(opcode.NE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(Compare(a, b) != 0)
		return e, nil
	})

	Eq = types.NewOp(opcode.EQ, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(Compare(a, b) == 0)
		return e, nil
	})

	Ge = types.NewOp(opcode.GE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(Compare(a, b) >= 0)
		return e, nil
	})

	Le = types.NewOp(opcode.LE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(Compare(a, b) <= 0)
		return e, nil
	})

	ISNULL = types.NewOp(opcode.IsNull, 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 1 {
			panic("error param numbers")
		}
		a := v[0]
		e := parser_driver.ValueExpr{}
		boolA := ConvertToBoolOrNull(a)
		if boolA == -1 {
			e.SetValue(1)
			return e, nil
		}
		e.SetValue(0)
		return e, nil
	})

	NULLEq = types.NewOp(opcode.NullEQ, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a := v[0]
		b := v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull && b.Kind() == tidb_types.KindNull {
			e.SetValue(1)
			return e, nil
		}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetValue(0)
			return e, nil
		}
		e.SetValue(Compare(a, b) == 0)
		return e, nil
	})

	IN = types.NewOp(opcode.In, 1, -1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) < 2 {
			panic("error param numbers")
		}
		a := v[0]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		hasNull := false
		for _, b := range v[1:] {
			if b.Kind() == tidb_types.KindNull {
				hasNull = true
				continue
			}
			if Compare(a, b) == 0 {
				e.SetValue(1)
				return e, nil
			}
		}
		if hasNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(0)
		return e, nil
	})
)

func init() {
	// DONOT op on non-date format types
	for _, f := range []*types.Op{Lt, Gt, Le, Ge, Ne, Eq, NULLEq} {
		f.SetAcceptType(0, types.DatetimeArg, types.AnyArg^types.StringArg^types.IntArg^types.FloatArg)
		f.SetAcceptType(1, types.DatetimeArg, types.AnyArg^types.StringArg^types.IntArg^types.FloatArg)
		f.SetAcceptType(0, types.StringArg, types.AnyArg^types.DatetimeArg)
		f.SetAcceptType(1, types.StringArg, types.AnyArg^types.DatetimeArg)
		f.SetAcceptType(0, types.IntArg, types.AnyArg^types.DatetimeArg)
		f.SetAcceptType(1, types.IntArg, types.AnyArg^types.DatetimeArg)
		f.SetAcceptType(0, types.FloatArg, types.AnyArg^types.DatetimeArg)
		f.SetAcceptType(1, types.FloatArg, types.AnyArg^types.DatetimeArg)

		BinaryOps.Add(f)
	}
	UnaryOps.Add(ISNULL)
	MultiaryOps.Add(IN)
}
