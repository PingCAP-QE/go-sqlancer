package generator

import (
	"strings"

	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/generator/hint"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
)

type Generator struct {
	Config
	Tables []types.Table
}

func (g *Generator) SelectStmt(genCtx *GenCtx, depth int) (*ast.SelectStmt, string, []types.Column, map[string]*connection.QueryItem, error) {
	selectStmtNode := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{},
		},
	}

	selectStmtNode.From = g.TableRefsClause(genCtx)
	g.walkTableRefs(selectStmtNode.From.TableRefs, genCtx)

	selectStmtNode.Where = g.ConditionClause(genCtx, depth)
	selectStmtNode.TableHints = g.tableHintsExpr(genCtx.UsedTables)

	columnInfos, updatedPivotRows := g.walkResultFields(selectStmtNode, genCtx)
	sql, err := BufferOut(selectStmtNode)
	return selectStmtNode, sql, columnInfos, updatedPivotRows, err
}

func (g *Generator) ConditionClause(ctx *GenCtx, depth int) ast.ExprNode {
	// TODO: support subquery
	// TODO: more ops
	exprType := types.TypeNumberLikeArg
	var err error
	retry := 2
	node, val, err := g.generateExpr(ctx, exprType, depth)
	for err != nil && retry > 0 {
		log.L().Error("generate where expr error", zap.Error(err))
		node, val, err = g.generateExpr(ctx, exprType, depth)
		retry--
	}
	if err != nil {
		panic("retry times exceed 3")
	}

	return g.rectifyCondition(node, val)
}

func (g *Generator) rectifyCondition(node ast.ExprNode, val parser_driver.ValueExpr) ast.ExprNode {
	// pthese := ast.ParenthesesExpr{}
	// pthese.Expr = node
	switch val.Kind() {
	case tidb_types.KindNull:
		node = &ast.IsNullExpr{
			// Expr: &pthese,
			Expr: node,
			Not:  false,
		}
	default:
		// make it true
		zero := parser_driver.ValueExpr{}
		zero.SetValue(false)
		if util.CompareValueExpr(val, zero) == 0 {
			node = &ast.UnaryOperationExpr{
				Op: opcode.Not,
				// V:  &pthese,
				V: node,
			}
		} else {
			// make it return 1 as true through add IS NOT TRUE
			node = &ast.IsNullExpr{
				// Expr: &pthese,
				Expr: node,
				Not:  true,
			}
		}
	}
	// remove useless parenthese
	if n, ok := node.(*ast.ParenthesesExpr); ok {
		node = n.Expr
	}
	return node
}

func (g *Generator) walkResultFields(node *ast.SelectStmt, genCtx *GenCtx) ([]types.Column, map[string]*connection.QueryItem) {
	if genCtx.IsNoRECMode {
		exprNode := &parser_driver.ValueExpr{}
		tp := tidb_types.NewFieldType(mysql.TypeLonglong)
		tp.Flag = 128
		exprNode.TexprNode.SetType(tp)
		exprNode.Datum.SetInt64(1)
		countField := ast.SelectField{
			Expr: &ast.AggregateFuncExpr{
				F: "count",
				Args: []ast.ExprNode{
					exprNode,
				},
			},
		}
		node.Fields.Fields = append(node.Fields.Fields, &countField)
		return nil, nil
	}
	columns := make([]types.Column, 0)
	row := make(map[string]*connection.QueryItem)
	for _, table := range genCtx.ResultTables {
		for _, column := range table.Columns {
			asname := genCtx.createTmpColumn()
			selectField := ast.SelectField{
				Expr:   column.ToModel(),
				AsName: model.NewCIStr(asname),
			}
			node.Fields.Fields = append(node.Fields.Fields, &selectField)
			col := column.Clone()
			col.AliasName = types.CIStr(asname)
			columns = append(columns, col)
			row[asname] = genCtx.PivotRows[column.String()]
		}
	}
	return columns, row
}

// TableRefsClause generates linear joins
func (g *Generator) TableRefsClause(genCtx *GenCtx) *ast.TableRefsClause {
	clause := &ast.TableRefsClause{TableRefs: &ast.Join{
		Left:  &ast.TableName{},
		Right: &ast.TableName{},
	}}
	usedTables := genCtx.UsedTables
	var node = clause.TableRefs
	// TODO: it works, but need to refactor
	if len(usedTables) == 1 {
		clause.TableRefs = &ast.Join{
			Left: &ast.TableSource{
				Source: &ast.TableName{
					Name: usedTables[0].Name.ToModel(),
				},
			},
			Right: nil,
		}
	}
	for i := len(usedTables) - 1; i >= 1; i-- {
		var tp ast.JoinType
		if !genCtx.EnableLeftRightJoin {
			tp = ast.CrossJoin
		} else {
			switch Rd(3) {
			case 0:
				tp = ast.RightJoin
			case 1:
				tp = ast.LeftJoin
			default:
				tp = ast.CrossJoin
			}
		}
		node.Right = &ast.TableSource{
			Source: &ast.TableName{
				Name: usedTables[i].Name.ToModel(),
			},
		}
		node.Tp = tp
		if i == 1 {
			node.Left = &ast.TableSource{
				Source: &ast.TableName{
					Name: usedTables[i-1].Name.ToModel(),
				},
			}
		} else {
			node.Left = &ast.Join{}
			node = node.Left.(*ast.Join)
		}
	}
	return clause
}

func (g *Generator) tableHintsExpr(usedTables []types.Table) []*ast.TableOptimizerHint {
	hints := make([]*ast.TableOptimizerHint, 0)
	if !g.Hint {
		return hints
	}
	// avoid duplicated hints
	enabledHints := make(map[string]bool)
	length := Rd(4)
	for i := 0; i < length; i++ {
		to := hint.GenerateHintExpr(usedTables)
		if to == nil {
			continue
		}
		if _, ok := enabledHints[to.HintName.String()]; !ok {
			hints = append(hints, to)
			enabledHints[to.HintName.String()] = true
		}
	}
	return hints
}

// NOTICE: not support multi-table update
func (g *Generator) UpdateStmt(tables []types.Table, currTable types.Table) (string, error) {
	for _, t := range tables {
		if t.Name.Eq(currTable.Name) {
			goto gCtxUpdate
		}
	}
	tables = append(tables, currTable)
gCtxUpdate:
	gCtx := NewGenCtx(tables, nil)
	gCtx.IsInUpdateDeleteStmt = true
	gCtx.EnableLeftRightJoin = false
	node := &ast.UpdateStmt{
		Where:     g.ConditionClause(gCtx, 2),
		IgnoreErr: true,
		TableRefs: g.TableRefsClause(gCtx),
		List:      make([]*ast.Assignment, 0),
	}
	// remove id col
	tempColumns := make(types.Columns, 0)
	for _, c := range currTable.Columns {
		if !strings.HasPrefix(c.Name.String(), "id") {
			tempColumns = append(tempColumns, c)
		}
	}

	// number of SET assignments
	for i := Rd(3) + 1; i > 0; i-- {
		asn := ast.Assignment{}
		col := tempColumns.RandColumn()

		asn.Column = col.ToModel().Name
		argTp := TransStringType(col.Type)
		if RdBool() {
			asn.Expr, _ = g.constValueExpr(argTp)
		} else {
			var err error
			asn.Expr, _, err = g.columnExpr(gCtx, argTp)
			if err != nil {
				log.L().Warn("columnExpr returns error", zap.Error(err))
				asn.Expr, _ = g.constValueExpr(argTp)
			}
		}
		node.List = append(node.List, &asn)
	}
	// TODO: add hints

	if sql, err := BufferOut(node); err != nil {
		return "", errors.Trace(err)
	} else {
		return sql, nil
	}
}

// NOTICE: not support multi-table delete
func (g *Generator) DeleteStmt(tables []types.Table, currTable types.Table) (string, error) {
	for _, t := range tables {
		if t.Name.Eq(currTable.Name) {
			goto gCtxDelete
		}
	}
	tables = append(tables, currTable)
gCtxDelete:
	gCtx := NewGenCtx(tables, nil)
	gCtx.IsInUpdateDeleteStmt = true
	gCtx.EnableLeftRightJoin = false
	node := &ast.DeleteStmt{
		Where:        g.ConditionClause(gCtx, 2),
		IgnoreErr:    true,
		IsMultiTable: true,
		BeforeFrom:   true,
		TableRefs:    g.TableRefsClause(gCtx),
		Tables: &ast.DeleteTableList{
			Tables: []*ast.TableName{{Name: currTable.Name.ToModel()}},
		},
	}
	// random some tables in UsedTables to be delete
	// deletedTables := tables[:RdRange(1, int64(len(tables)))]
	// fmt.Printf("%+v", node.Tables.Tables[0])
	// for _, t := range deletedTables {
	// 	node.Tables.Tables = append(node.Tables.Tables, &ast.TableName{Name: t.Name.ToModel()})
	// }
	// TODO: add hint
	if sql, err := BufferOut(node); err != nil {
		return "", errors.Trace(err)
	} else {
		return sql, nil
	}
}
