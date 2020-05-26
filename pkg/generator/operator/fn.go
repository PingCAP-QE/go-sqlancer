package operator

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
)

var (
	defaultFuncCallNodeCb = func(cb types.GenNodeCb, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{}, errors.New(fmt.Sprintf("cannot find valid param for type(%d) returned", ret))
		}
		arg := argList[rand.Intn(len(argList))]
		var v []parser_driver.ValueExpr
		var args []ast.ExprNode
		for i := 0; i < len(arg); i++ {
			expr, value, err := cb(arg[i])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			v = append(v, value)
			args = append(args, expr)
		}
		value, err := op.Eval(v...)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.FuncCallExpr{
			FnName: model.NewCIStr(op.GetName()),
			Args:   args,
		}
		return node, value, nil
	}
)
