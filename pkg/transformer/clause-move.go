package transformer

import "github.com/pingcap/parser/ast"

var (
	// TODO: implement where-to-on Transformer
	Where2On TransformerSingleton = func(nodes []ast.ResultSetNode) []ast.ResultSetNode {
		return nodes
	}

	// TODO: implement on-to-where Transformer
	On2Where TransformerSingleton = func(nodes []ast.ResultSetNode) []ast.ResultSetNode {
		return nodes
	}
)
