package equtrans

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
)

type TLPType = uint8

const (
	WHERE TLPType = iota
	ON_CONDITION
	HAVING
)

type TLPTrans struct {
	Expr ast.ExprNode
	Tp   TLPType
}

func (t *TLPTrans) Trans(stmt *ast.SelectStmt) ast.ResultSetNode {
	if t.Expr == nil {
		return stmt
	}

	var selects []*ast.SelectStmt

	switch t.Tp {
	case WHERE:
		selects = t.transWhere(stmt)
	case ON_CONDITION:
		if stmt.From != nil && stmt.From.TableRefs != nil && stmt.From.TableRefs.Right != nil {
			selects = t.transOnCondition(stmt)
		} else {
			return stmt
		}
	case HAVING:
		if stmt.GroupBy != nil {
			selects = t.transHaving(stmt)
		} else {
			return stmt
		}
	}

	for i, selectStmt := range selects {
		if i != 0 {
			selectStmt.IsAfterUnionDistinct = false
		}
	}

	return &ast.UnionStmt{
		SelectList: &ast.UnionSelectList{
			Selects: selects,
		}}
}

func (t *TLPTrans) transHaving(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		if selectStmt.Having == nil {
			selectStmt.Having = &ast.HavingClause{Expr: expr}
		} else {
			selectStmt.Having = &ast.HavingClause{Expr: &ast.BinaryOperationExpr{Op: opcode.And, L: stmt.Having.Expr, R: expr}}
		}
		selects = append(selects, &selectStmt)
	}
	return selects
}

func (t *TLPTrans) transOnCondition(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		tableRefs := *stmt.From.TableRefs
		selectStmt.From = &ast.TableRefsClause{
			TableRefs: &tableRefs,
		}
		if selectStmt.From.TableRefs.On == nil {
			selectStmt.From.TableRefs.On = &ast.OnCondition{Expr: expr}
		} else {
			selectStmt.From.TableRefs.On = &ast.OnCondition{Expr: &ast.BinaryOperationExpr{Op: opcode.And, L: stmt.From.TableRefs.On.Expr, R: expr}}
		}
		selects = append(selects, &selectStmt)
	}
	return selects
}

func (t *TLPTrans) transWhere(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		if selectStmt.Where == nil {
			selectStmt.Where = expr
		} else {
			selectStmt.Where = &ast.BinaryOperationExpr{Op: opcode.And, L: stmt.Where, R: expr}
		}
		selects = append(selects, &selectStmt)
	}
	return selects
}

func partition(expr ast.ExprNode) []ast.ExprNode {
	isFalse := &ast.IsTruthExpr{Expr: expr}
	isTrue := *isFalse
	isTrue.True = 1
	return []ast.ExprNode{&isTrue, isFalse, &ast.IsNullExpr{Expr: expr}}
}
