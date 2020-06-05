package transformer

import (
	"errors"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	"go.uber.org/zap"
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

func (t *TLPTrans) Transform(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
	resultSetNodes := nodeSet
	for idx, nodes := range nodeSet {
		nodeArr := nodes
		for _, node := range nodes {
			switch n := node.(type) {
			case *ast.UnionStmt:
			case *ast.SelectStmt:
				if eqNode, err := t.transOneStmt(n); err == nil {
					nodeArr = append(nodeArr, eqNode)
				} else {
					log.L().Info("tlp trans error", zap.Error(err))
				}
			default:
				panic("type not implemented")
			}
		}
		resultSetNodes[idx] = nodeArr
	}
	return resultSetNodes
}

func (t *TLPTrans) transOneStmt(stmt *ast.SelectStmt) (ast.ResultSetNode, error) {
	if t.Expr == nil {
		return nil, errors.New("no expr")
	}

	// an aggregate func may add some usless empty rows
	// such as [3] and [NULL, NULL, 3]
	if stmt.Fields.Fields != nil {
		for _, field := range stmt.Fields.Fields {
			// TODO: cannot avoid cases like `count(*) + 1`
			if _, ok := field.Expr.(*ast.AggregateFuncExpr); ok {
				return nil, errors.New("any aggregation func should not be in stmt")
			}
		}
	}

	var selects []*ast.SelectStmt

	switch t.Tp {
	case WHERE:
		selects = t.transWhere(stmt)
	case ON_CONDITION:
		if stmt.From != nil && stmt.From.TableRefs != nil && stmt.From.TableRefs.Right != nil {
			selects = t.transOnCondition(stmt)
		} else {
			return nil, errors.New("from clause is invalid")
		}
	case HAVING:
		if stmt.GroupBy != nil {
			selects = t.transHaving(stmt)
		} else {
			return nil, errors.New("group by is empty but has having")
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
		}}, nil
}

func (t *TLPTrans) transHaving(stmt *ast.SelectStmt) []*ast.SelectStmt {
	selects := make([]*ast.SelectStmt, 0, 3)
	for _, expr := range partition(t.Expr) {
		selectStmt := *stmt
		if selectStmt.Having == nil {
			selectStmt.Having = &ast.HavingClause{Expr: expr}
		} else {
			selectStmt.Having = &ast.HavingClause{Expr: &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: stmt.Having.Expr, R: expr}}
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
			selectStmt.From.TableRefs.On = &ast.OnCondition{Expr: &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: stmt.From.TableRefs.On.Expr, R: expr}}
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
			selectStmt.Where = &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: stmt.Where, R: expr}
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
