package equtrans

import "github.com/pingcap/parser/ast"

// TODO: implement where-to-on transformer
func where2On(stmt *ast.SelectStmt) *ast.SelectStmt {
	return stmt
}

// TODO: implement on-to-where transformer
func on2Where(stmt *ast.SelectStmt) *ast.SelectStmt {
	return stmt
}
