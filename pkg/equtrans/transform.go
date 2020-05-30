package equtrans

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	UnaryTransformer func(stmt *ast.SelectStmt) *ast.SelectStmt

	ComposeTransformer func(stmts ...*ast.SelectStmt) *ast.SelectStmt

	TransCtx struct {
		Depth int
		Step  int
	}
)

var (
	unaryTransformers   = []UnaryTransformer{noRECTrans, tlpTrans}
	composeTransformers = []ComposeTransformer{intersectTrans, unionTrans}
)

func Trans(stmt *ast.SelectStmt, ctx *TransCtx) *ast.SelectStmt {
	result := stmt
	for depth := ctx.Depth; depth > 0; depth -= ctx.Step {
		stmts := make([]*ast.SelectStmt, ctx.Step+1)
		stmts = append(stmts, result)
		for i := 0; i < ctx.Step; i++ {
			unary := unaryTransformers[util.Rd(len(unaryTransformers))]
			stmts = append(stmts, unary(stmt))
		}
		compose := composeTransformers[util.Rd(len(composeTransformers))]
		result = compose(stmts...)
	}
	return result
}
