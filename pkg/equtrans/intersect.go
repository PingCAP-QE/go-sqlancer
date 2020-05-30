package equtrans

import "github.com/pingcap/parser/ast"

// TODO: implement intersect transformer
func intersectTrans(stmts ...*ast.SelectStmt) *ast.SelectStmt {
	return new(ast.SelectStmt)
}
