package equtrans

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	Transformer interface {
		Trans(stmt *ast.SelectStmt) *ast.SelectStmt
	}

	TransformFunc func(stmt *ast.SelectStmt) *ast.SelectStmt

	TransCtx struct {
		Segments int
	}
)

func (t TransformFunc) Trans(stmt *ast.SelectStmt) *ast.SelectStmt {
	return t(stmt)
}

func Trans(transformers []Transformer, stmt *ast.SelectStmt, Segments int) ast.DMLNode {
	var random = func() Transformer {
		return transformers[util.Rd(len(transformers))]
	}

	if Segments == 1 {
		return random().Trans(stmt)
	}

	stmts := make([]*ast.SelectStmt, Segments)
	for i := 0; i < Segments; i++ {
		transformer := random()
		stmts = append(stmts, transformer.Trans(stmt))
	}
	return &ast.UnionStmt{SelectList: &ast.UnionSelectList{Selects: stmts}}
}
