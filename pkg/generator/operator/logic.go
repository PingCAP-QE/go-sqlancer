package operator

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/opcode"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	LogicOps = make(types.OpFuncIndex)

	LogicXor = types.NewOp(opcode.LogicXor, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
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
		e.SetValue(ConvertToBoolOrNull(a) != ConvertToBoolOrNull(b))
		return e, nil
	})

	LogicAnd = types.NewOp(opcode.LogicAnd, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
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
	})

	LogicOr = types.NewOp(opcode.LogicOr, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
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
	})

	Not = types.NewOp(opcode.Not, 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
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
	})
)

func init() {
	for _, f := range []*types.Op{LogicXor, LogicAnd, LogicOr} {
		BinaryOps.Add(f)
		LogicOps.Add(f)
	}

	UnaryOps.Add(Not)
	LogicOps.Add(Not)
}
