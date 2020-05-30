package equtrans

import "github.com/pingcap/parser/ast"

// TODO: implement no-rec transformer
func noRECTrans(stmt *ast.SelectStmt) *ast.SelectStmt {
	return stmt
}
