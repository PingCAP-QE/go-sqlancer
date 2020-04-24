package operator

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/opcode"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	BinaryOps types.OpFuncIndex = make(types.OpFuncIndex)

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
)

func init() {
	// DONOT op on non-date format types
	for _, f := range []*types.Op{Lt, Gt, Le, Ge, Ne, Eq} {
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
}
