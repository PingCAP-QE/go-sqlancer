package equtrans

import "github.com/pingcap/parser/ast"

type TLPTrans struct {
}

func (t *TLPTrans) Trans(stmt *ast.SelectStmt) *ast.SelectStmt {
	return stmt
}
