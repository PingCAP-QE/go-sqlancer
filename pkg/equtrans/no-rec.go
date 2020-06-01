package equtrans

import "github.com/pingcap/parser/ast"

// TODO: implement no-rec Transformer
var NoRECTrans TransformFunc = func(stmt *ast.SelectStmt) *ast.SelectStmt {
	return stmt
}
