package equtrans

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	Transformer interface {
		Trans([][]ast.ResultSetNode) [][]ast.ResultSetNode
	}

	TransformFunc func([][]ast.ResultSetNode) [][]ast.ResultSetNode
)

func (t TransformFunc) Trans(stmts [][]ast.ResultSetNode) [][]ast.ResultSetNode {
	return t(stmts)
}

func Trans(transformers []Transformer, stmt ast.ResultSetNode, Depth int) [][]ast.ResultSetNode {
	var random = func() Transformer {
		return transformers[util.Rd(len(transformers))]
	}

	results := [][]ast.ResultSetNode{{stmt}}
	// if Depth == 1 {
	// 	random().Trans(results)
	// 	return results
	// }

	for i := 0; i < Depth; i++ {
		results = random().Trans(results)
	}
	// resultSets = append(resultSets, union(resultSets))
	return results
}
