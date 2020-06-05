package equtrans

import "github.com/pingcap/parser/ast"

var (
	// TODO: implement where-to-on Transformer
	Where2On TransformFunc = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
		return nodeSet
	}

	// TODO: implement on-to-where Transformer
	On2Where TransformFunc = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
		return nodeSet
	}
)
