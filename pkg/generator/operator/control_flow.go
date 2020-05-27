package operator

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
)

// https://dev.mysql.com/doc/refman/8.0/en/control-flow-functions.html#operator_case
var (
	// we limit case only two branches and no else branch here, so the min arg count is 2 and the max is 4
	// CASE WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...] END
	Case = types.NewFn("CASE", 2, 4, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 && len(v) != 4 {
			panic("error params number")
		}
		e := parser_driver.ValueExpr{}
		e.SetNull()
		for compareValueIdx := 0; compareValueIdx < len(v); compareValueIdx += 2 {
			compareValue := v[compareValueIdx]
			resultValue := v[compareValueIdx+1]
			t := parser_driver.ValueExpr{}
			t.SetValue(1)
			if util.Compare(t, compareValue) == 0 {
				return resultValue, nil
			}
		}
		return e, nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		// compare all compare value's type, and we don't consider implicit type cast for the sake of simplicity
		whenTp := argTyps[0]
		for i := 0; i < len(argTyps); i += 2 {
			if whenTp != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		// And we compare all return values in the same way
		valueTp := argTyps[1]
		for i := 1; i < len(argTyps); i += 2 {
			if valueTp != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		return valueTp, false, nil
	}, func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{}, errors.New(fmt.Sprintf("cannot find valid param for type(%d) returned", ret))
		}
		whenCount := int(util.RdRange(1, 2))
		arg := argList[rand.Intn(len(argList))]
		var whenClauses []*ast.WhenClause
		var v []parser_driver.ValueExpr
		for i := 0; i < whenCount; i++ {
			whenNode, value, err := cb(arg[whenCount*2])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			whenResultNode, resultValue, err := cb(arg[whenCount*2+1])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			whenClauses = append(whenClauses, &ast.WhenClause{
				Expr:   whenNode,
				Result: whenResultNode,
			})
			v = append(v, value, resultValue)
		}
		node := &ast.CaseExpr{
			Value:       nil,
			WhenClauses: whenClauses,
			ElseClause:  nil,
		}
		result, err := op.Eval(v...)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		return node, result, nil
	})

	// IF(expr1,expr2,expr3)
	// If expr1 is TRUE (expr1 <> 0 and expr1 <> NULL), IF() returns expr2. Otherwise, it returns expr3.
	If = types.NewFn("IF", 3, 3, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 3 {
			panic("error params number")
		}
		expr1 := v[0]
		t := parser_driver.ValueExpr{}
		t.SetValue(true)
		if expr1.Kind() != tidb_types.KindNull && util.Compare(expr1, t) == 0 {
			return v[1], nil
		}
		return v[2], nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		expr1Tp := argTyps[0]
		expr2Tp := argTyps[1]
		expr3Tp := argTyps[2]
		if expr2Tp != expr3Tp {
			return 0, false, errors.New("invalid type")
		}
		switch expr1Tp {
		case types.TypeFloatArg, types.TypeIntArg:
			return expr2Tp, false, nil
		default:
			return expr2Tp, true, nil
		}
	}, defaultFuncCallNodeCb)

	// IFNULL(expr1,expr2)
	// If expr1 is not NULL, IFNULL() returns expr1; otherwise it returns expr2.
	IfNull = types.NewFn("IFNULL", 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error params number")
		}
		expr1 := v[0]
		expr2 := v[1]
		if expr1.Kind() != tidb_types.KindNull {
			return expr1, nil
		}
		return expr2, nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		expr1Tp := argTyps[0]
		expr2Tp := argTyps[1]
		if expr1Tp != expr2Tp {
			return 0, false, errors.New("invalid type")
		}
		return expr1Tp, false, nil
	}, defaultFuncCallNodeCb)

	// NULLIF(expr1,expr2)
	// Returns NULL if expr1 = expr2 is true, otherwise returns expr1. This is the same as CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END.
	NullIf = types.NewFn("NULLIF", 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error params number")
		}
		expr1 := v[0]
		expr2 := v[1]
		e := parser_driver.ValueExpr{}
		e.SetNull()
		if expr1.Kind() == tidb_types.KindNull || expr2.Kind() == tidb_types.KindNull {
			return e, nil
		}
		if util.Compare(expr1, expr2) == 0 {
			return e, nil
		}
		return expr1, nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		expr1Tp := argTyps[0]
		expr2Tp := argTyps[1]
		if expr1Tp != expr2Tp {
			return 0, false, errors.New("invalid type")
		}
		return expr1Tp, false, nil
	}, defaultFuncCallNodeCb)
)
