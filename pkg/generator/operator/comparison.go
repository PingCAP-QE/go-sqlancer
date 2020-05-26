package operator

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
)

var (
	// NULL-safe equal. This operator performs an equality comparison like the = operator,
	// but returns 1 rather than NULL if both operands are NULL, and 0 rather than NULL if one operand is NULL.
	NullEq = types.NewOp(opcode.NullEQ, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull && b.Kind() == tidb_types.KindNull {
			e.SetValue(1)
			return e, nil
		}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetValue(0)
			return e, nil
		}
		e.SetValue(util.Compare(a, b) == 0)
		return e, nil
	}, defaultBinaryOpValidate, defaultBinaryOpGenNode(opcode.NullEQ))

	// Tests whether a value is NULL.
	IsNull = types.NewOp(opcode.IsNull, 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 1 {
			panic("error param numbers")
		}
		e := parser_driver.ValueExpr{}
		e.SetValue(v[0].Kind() == tidb_types.KindNull)
		return e, nil
	}, func(u ...uint64) (uint64, bool, error) {
		return u[0], false, nil
	}, func(cb types.GenNodeCb, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{}, errors.New(fmt.Sprintf("cannot find valid param for type(%d) returned", ret))
		}
		arg := argList[rand.Intn(len(argList))]
		expr, value, err := cb(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		value, err = op.Eval(value)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.IsNullExpr{
			Expr: expr,
			Not:  false,
		}
		return node, value, nil
	})

	// expr IN (value,...)
	In = types.NewOp(opcode.In, 2, 5, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) < 2 {
			panic("error param numbers")
		}
		expr := v[0]
		e := parser_driver.ValueExpr{}
		if expr.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		hasNull := false
		for _, b := range v[1:] {
			if b.Kind() == tidb_types.KindNull {
				hasNull = true
				continue
			}
			if util.Compare(expr, b) == 0 {
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
	}, func(argTyps ...uint64) (uint64, bool, error) {
		exprType := argTyps[0]
		for i := 1; i < len(argTyps); i++ {
			if exprType != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		return types.TypeIntArg | types.TypeFloatArg, false, nil
	}, func(cb types.GenNodeCb, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{}, errors.New(fmt.Sprintf("cannot find valid param for type(%d) returned", ret))
		}
		arg := argList[rand.Intn(len(argList))]
		expr, value, err := cb(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		var list []ast.ExprNode
		v := []parser_driver.ValueExpr{value}
		for i := 1; i < len(arg); i++ {
			expr, value, err := cb(arg[i])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			list = append(list, expr)
			v = append(v, value)
		}
		value, err = op.Eval(v...)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.PatternInExpr{
			Expr: expr,
			List: list,
		}
		return node, value, nil
	})
)
