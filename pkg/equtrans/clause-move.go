package equtrans

import "github.com/pingcap/parser/ast"

// TODO: implement where-to-on Transformer
func where2On(stmt *ast.SelectStmt) ast.ResultSetNode {
	return stmt
}

// TODO: implement on-to-where Transformer
func on2Where(stmt *ast.SelectStmt) ast.ResultSetNode {
	return stmt
}
