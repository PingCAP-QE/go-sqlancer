package operator

import (
	"fmt"
	"math/rand"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	UnaryOps types.OpFuncMap = make(types.OpFuncMap)

	Not = types.NewOp(opcode.Not, 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		// evaluation
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
	}, func(args ...uint64) (uint64, bool, error) {
		// checking args validate?
		if len(args) != 1 {
			panic("require only one param")
		}
		arg := args[0]
		if arg&^(types.TypeDatatimeLikeArg|types.TypeNumberLikeArg) == 0 {
			return types.TypeIntArg | types.TypeFloatArg, false, nil
		}
		if arg&^(types.TypeNonFormattedStringArg) == 0 {
			return types.TypeIntArg | types.TypeFloatArg, true, nil
		}
		panic("unreachable")
	}, func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		// generate op node and call cb to generate its child node
		val := parser_driver.ValueExpr{}
		op, ok := this.(*types.BaseOpFunc)
		if !ok {
			panic("should can transfer to BaseOpFunc")
		}

		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, val, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, val,
				errors.New(fmt.Sprintf("cannot find valid param for type(%d) returned", ret))
		}
		firstArg := argList[rand.Intn(len(argList))][0]

		node := ast.UnaryOperationExpr{}
		node.Op = opcode.Not
		node.V, val, err = cb(firstArg)
		if err != nil {
			return nil, val, errors.Trace(err)
		}
		evalResult, err := op.Eval(val)
		if err != nil {
			return nil, val, errors.Trace(err)
		}
		return &node, evalResult, nil
	})
)

func init() {
	UnaryOps.Add(Not)
	RegisterToOpFnIndex(Not)
}
