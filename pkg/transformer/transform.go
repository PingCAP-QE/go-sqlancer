package transformer

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
)

type (
	Transformer interface {
		Transform([]ast.ResultSetNode) []ast.ResultSetNode
	}

	TransformerSingleton func([]ast.ResultSetNode) []ast.ResultSetNode
)

func (t TransformerSingleton) Transform(stmts []ast.ResultSetNode) []ast.ResultSetNode {
	return t(stmts)
}

func RandTransformer(transformers ...Transformer) Transformer {
	return transformers[util.Rd(len(transformers))]
}
