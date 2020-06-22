package transformer

import "github.com/pingcap/parser/ast"

var (
	// TODO: implement where-to-on Transformer
	Where2On TransformerSingleton = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
		return nodeSet
	}

	// TODO: implement on-to-where Transformer
	On2Where TransformerSingleton = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
		return nodeSet
	}
)
