package transformer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"
	"go.uber.org/zap"

	"github.com/chaos-mesh/go-sqlancer/pkg/util"
)

type TLPType = uint8

const (
	WHERE TLPType = iota
	ON_CONDITION
	HAVING
)

var (
	TLPTypes          = [...]TLPType{WHERE, ON_CONDITION, HAVING}
	SelfComposableMap = map[string]bool{ast.AggFuncMax: true, ast.AggFuncMin: true, ast.AggFuncSum: true}
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

	var selects []*ast.SelectStmt

	switch t.Tp {
	case WHERE:
		selects = t.transWhere(stmt)
	case ON_CONDITION:
		// only cross join is valid in on-condition transform
		if stmt.From != nil && stmt.From.TableRefs != nil && stmt.From.TableRefs.Right != nil && stmt.From.TableRefs.Tp == ast.CrossJoin {
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

	if stmt.Distinct {
		for _, selectStmt := range selects {
			selectStmt.Distinct = false
			selectStmt.IsAfterUnionDistinct = true

		}
	} else {
		for _, selectStmt := range selects {
			selectStmt.IsAfterUnionDistinct = false
		}
	}

	// try aggregate transform
	return tryAggTransform(stmt, &ast.UnionStmt{
		SelectList: &ast.UnionSelectList{
			Selects: selects,
		}})

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

func RandTLPType() TLPType {
	return TLPTypes[util.Rd(len(TLPTypes))]
}

func tryAggTransform(selectStmt *ast.SelectStmt, unionStmt *ast.UnionStmt) (ast.ResultSetNode, error) {
	if selectStmt.Fields != nil && len(selectStmt.Fields.Fields) != 0 {
		aggFns := make([]int, 0)
		exprNames := make(map[string]bool)
		selectFields := make([]*ast.SelectField, 0, len(selectStmt.Fields.Fields))
		unionFields := make([]*ast.SelectField, 0, len(selectStmt.Fields.Fields))
		for index, field := range selectStmt.Fields.Fields {
			selectField, unionField := *field, *field
			selectFields = append(selectFields, &selectField)
			unionFields = append(unionFields, &unionField)
			if !field.Auxiliary {
				if field.AsName.String() != "" {
					exprNames[field.AsName.String()] = true
				}
				if fn, ok := field.Expr.(*ast.AggregateFuncExpr); ok {
					if SelfComposableMap[strings.ToLower(fn.F)] && !fn.Distinct {
						aggFns = append(aggFns, index)
						selectFn := *fn
						selectField.Expr = &selectFn
						continue
					} else {
						return nil, errors.New("only self-composable aggregation func is supported")
					}
				}
			}
		}

		if len(aggFns) != 0 {
			for _, index := range aggFns {
				if unionFields[index].AsName.String() == "" {
					name := chooseName(index, exprNames)
					unionFields[index].AsName = model.NewCIStr(name)
				}
				selectFields[index].AsName = model.NewCIStr("")
				selectFields[index].Expr.(*ast.AggregateFuncExpr).Args = []ast.ExprNode{
					&ast.ColumnNameExpr{
						Name: &ast.ColumnName{
							Name: unionFields[index].AsName,
						},
					},
				}
			}
			for _, stmt := range unionStmt.SelectList.Selects {
				stmt.Fields = &ast.FieldList{Fields: unionFields}
			}

			return &ast.SelectStmt{
				SelectStmtOpts: selectStmt.SelectStmtOpts,
				Fields:         &ast.FieldList{Fields: selectFields},
				From: &ast.TableRefsClause{TableRefs: &ast.Join{
					Left: &ast.TableSource{Source: unionStmt, AsName: model.NewCIStr("tmp")},
				}},
			}, nil
		}
	}
	return unionStmt, nil
}

func chooseName(index int, exprNames map[string]bool) string {
	for i := 1; ; i++ {
		name := fmt.Sprintf("c%d", i*index)
		if !exprNames[name] {
			return name
		}
	}
}
