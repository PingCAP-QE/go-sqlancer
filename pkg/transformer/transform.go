package transformer

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	Transformer interface {
		Transform([][]ast.ResultSetNode) [][]ast.ResultSetNode
	}

	TransformerSingleton func([][]ast.ResultSetNode) [][]ast.ResultSetNode
)

func (t TransformerSingleton) Transform(stmts [][]ast.ResultSetNode) [][]ast.ResultSetNode {
	return t(stmts)
}

func Transform(transformers []Transformer, stmt ast.ResultSetNode, depth int) [][]ast.ResultSetNode {
	var random = func() Transformer {
		return transformers[util.Rd(len(transformers))]
	}

	results := [][]ast.ResultSetNode{{stmt}}
	// if depth == 1 {
	// 	random().Transform(results)
	// 	return results
	// }

	for i := 0; i < depth; i++ {
		results = random().Transform(results)
	}
	// resultSets = append(resultSets, union(resultSets))
	return results
}
