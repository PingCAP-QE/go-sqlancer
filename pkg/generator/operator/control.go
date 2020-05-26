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

var (
	// we limit case only two branches and no else branch here, so the min arg count is 3 and the max is 5
	// CASE value WHEN [compare_value] THEN result [WHEN [compare_value] THEN result ...] END
	Case = types.NewFn("CASE", 3, 5, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) < 3 || len(v) > 5 || len(v)%2 != 1 {
			panic("error params number")
		}
		e := parser_driver.ValueExpr{}
		caseValue := v[0]
		// if caseValue is a null caseValue, no branches' compare_value can match it, so we return null directly because there's no else branch.
		if caseValue.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		for compareValueIdx := 1; compareValueIdx < len(v); compareValueIdx += 2 {
			compareValue := v[compareValueIdx]
			resultValue := v[compareValueIdx+1]
			if util.Compare(caseValue, compareValue) == 0 {
				return resultValue, nil
			}
		}
		e.SetNull()
		return e, nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		// compare all compare value's type, and we don't consider implicit type cast for the sake of simplicity
		caseTp := argTyps[0]
		for i := 1; i < len(argTyps); i += 2 {
			if caseTp != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		// And we compare all return values in the same way
		valueTp := argTyps[2]
		for i := 2; i < len(argTyps); i += 2 {
			if valueTp != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		return valueTp, false, nil
	}, func(cb types.GenNodeCb, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
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
		caseNode, caseValue, err := cb(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		var whenClauses []*ast.WhenClause
		var v = []parser_driver.ValueExpr{caseValue}
		for i := 0; i < whenCount; i++ {
			whenNode, value, err := cb(arg[whenCount*2+1])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			whenResultNode, resultValue, err := cb(arg[whenCount*2+2])
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
			Value:       caseNode,
			WhenClauses: whenClauses,
			ElseClause:  nil,
		}
		result, err := op.Eval(v...)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		return node, result, nil
	})
)
