package transformer

import (
	"github.com/pingcap/parser/ast"
)

// TODO: implement union Transformer
var UnionTrans TransformerSingleton = func(nodes []ast.ResultSetNode) []ast.ResultSetNode {
	if len(nodes) > 1 {
		nodes = append(nodes, union(nodes))
	}
	return nodes
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
