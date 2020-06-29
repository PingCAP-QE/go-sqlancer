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

func NewTransformer(transformers []Transformer, depth int) Transformer {
	var random = func() Transformer {
		return transformers[util.Rd(len(transformers))]
	}

	var transformer Transformer = BlankTransformer

	for i := 0; i < depth; i++ {
		transformer = Join(transformer, random())
	}

	return transformer
}

var (
	BlankTransformer TransformerSingleton = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
		return nodeSet
	}
)
