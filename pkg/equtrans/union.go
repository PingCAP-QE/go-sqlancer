package equtrans

import "github.com/pingcap/parser/ast"

// TODO: implement union transformer
func unionTrans(stmts ...*ast.SelectStmt) *ast.SelectStmt {
	return new(ast.SelectStmt)
}
