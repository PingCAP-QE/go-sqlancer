package equtrans

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	Transformer func(stmt *ast.SelectStmt) *ast.SelectStmt

	TransCtx struct {
		Depth int
	}
)

var (
	transformers = []Transformer{noRECTrans, tlpTrans}
)

func randomTransformer() Transformer {
	return transformers[util.Rd(len(transformers))]
}

func Trans(stmt *ast.SelectStmt, ctx *TransCtx) ast.DMLNode {
	if ctx.Depth == 1 {
		return randomTransformer()(stmt)
	}

	stmts := make([]*ast.SelectStmt, ctx.Depth)
	for i := 0; i < ctx.Depth; i++ {
		transformer := randomTransformer()
		stmts = append(stmts, transformer(stmt))
	}
	return &ast.UnionStmt{SelectList: &ast.UnionSelectList{Selects: stmts}}
}
