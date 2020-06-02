package equtrans

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	Transformer interface {
		Trans(stmt *ast.SelectStmt) ast.ResultSetNode
	}

	TransformFunc func(stmt *ast.SelectStmt) ast.ResultSetNode
)

func (t TransformFunc) Trans(stmt *ast.SelectStmt) ast.ResultSetNode {
	return t(stmt)
}

func Trans(transformers []Transformer, stmt *ast.SelectStmt, Depth int) []ast.ResultSetNode {
	var random = func() Transformer {
		return transformers[util.Rd(len(transformers))]
	}

	if Depth == 1 {
		return []ast.ResultSetNode{random().Trans(stmt)}
	}

	resultSets := make([]ast.ResultSetNode, 0, Depth+1)
	for i := 0; i < Depth; i++ {
		transformer := random()
		resultSets = append(resultSets, transformer.Trans(stmt))
	}
	return resultSets
}
