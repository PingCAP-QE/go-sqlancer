package equtrans

import (
	"github.com/pingcap/parser/ast"
)

// TODO: implement union Transformer
var UnionTrans TransformFunc = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
	return nodeSet
}

func union(nodes []ast.ResultSetNode) *ast.UnionStmt {
	selects := make([]*ast.SelectStmt, 0)
	for _, node := range nodes {
		switch stmt := node.(type) {
		case *ast.SelectStmt:
			selects = append(selects, stmt)
		case *ast.UnionStmt:
			if stmt.SelectList != nil && stmt.SelectList.Selects != nil {
				selects = append(selects, stmt.SelectList.Selects...)
			}
		}
	}
	return &ast.UnionStmt{SelectList: &ast.UnionSelectList{Selects: selects}}
}
