package generator

import (
	"strings"

	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
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
	Tables           []types.Table
	allowColumnTypes []string

	Config
}

func (g *Generator) SelectStmtAst(genCtx *GenCtx, depth int) (ast.SelectStmt, error) {
	selectStmtNode := ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{},
		},
	}

	selectStmtNode.From = &ast.TableRefsClause{
		TableRefs: &ast.Join{
			Left:  &ast.TableName{},
			Right: &ast.TableName{},
		},
	}

	g.genResultSetNode(selectStmtNode.From.TableRefs, genCtx)
	g.genJoin(selectStmtNode.From.TableRefs, genCtx)
	selectStmtNode.Where = g.WhereClauseAst(genCtx, depth)
	selectStmtNode.TableHints = g.tableHintsExpr(genCtx.UsedTables)
	return selectStmtNode, nil
}

func (g *Generator) WhereClauseAst(ctx *GenCtx, depth int) ast.ExprNode {
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

// walk on select stmt
func (g *Generator) SelectStmt(node *ast.SelectStmt, genCtx *GenCtx) (string, []types.Column, map[string]*connection.QueryItem, error) {
	columnInfos, updatedPivotRows := g.walkResultFields(node, genCtx)
	// s.walkOrderByClause(node.OrderBy, table)
	// g.RectifyJoin(node.From.TableRefs, genCtx)
	// s.walkExprNode(node.Where, table, nil)
	sql, err := BufferOut(node)
	return sql, columnInfos, updatedPivotRows, err
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
		}
	}
	// remove useless parenthese
	if n, ok := node.(*ast.ParenthesesExpr); ok {
		node = n.Expr
	}
	return node
}

func (g *Generator) walkResultFields(node *ast.SelectStmt, genCtx *GenCtx) ([]types.Column, map[string]*connection.QueryItem) {
	columns := make([]types.Column, 0)
	rows := make(map[string]*connection.QueryItem)
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
			rows[asname] = genCtx.PivotRows[column.String()]
		}
	}
	return columns, rows
}

func (g *Generator) genResultSetNode(node *ast.Join, genCtx *GenCtx) {
	usedTables := genCtx.UsedTables
	l := len(usedTables)
	var left *ast.Join = node
	// TODO: it works, but need to refactory
	if l == 1 {
		ts := ast.TableSource{}
		tn := ast.TableName{}
		tn.Name = usedTables[0].Name.ToModel()
		ts.Source = &tn
		node.Left = &ts
		node.Right = nil
	}
	for i := l - 1; i >= 1; i-- {
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

		ts := ast.TableSource{}
		tn := ast.TableName{}
		tn.Name = usedTables[i].Name.ToModel()
		ts.Source = &tn
		if i > 1 {
			left.Tp = tp
			left.Right = &ts
			left.Left = &ast.Join{}
			left = left.Left.(*ast.Join)
		} else {
			left.Tp = tp
			left.Right = &ts
			ts2 := ast.TableSource{}
			tn2 := ast.TableName{}
			tn2.Name = usedTables[i-1].Name.ToModel()
			ts2.Source = &tn2
			left.Left = &ts2
		}
	}
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
func (g *Generator) UpdateDMLStmt(tables []types.Table, currTable types.Table) (string, error) {
	for _, t := range tables {
		if t.Name.Eq(currTable.Name) {
			goto GCTX_UPDATE
		}
	}
	tables = append(tables, currTable)
GCTX_UPDATE:
	gCtx := NewGenCtx(tables, nil)
	gCtx.IsInUpdateDeleteStmt = true
	gCtx.EnableLeftRightJoin = false
	node := &ast.UpdateStmt{
		Where:     g.WhereClauseAst(gCtx, 2),
		IgnoreErr: true,
		TableRefs: &ast.TableRefsClause{TableRefs: &ast.Join{}},
		List:      make([]*ast.Assignment, 0),
	}
	g.genResultSetNode(node.TableRefs.TableRefs, gCtx)

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
func (g *Generator) DeleteDMLStmt(tables []types.Table, currTable types.Table) (string, error) {
	for _, t := range tables {
		if t.Name.Eq(currTable.Name) {
			goto GCTX_DELETE
		}
	}
	tables = append(tables, currTable)
GCTX_DELETE:
	gCtx := NewGenCtx(tables, nil)
	gCtx.IsInUpdateDeleteStmt = true
	gCtx.EnableLeftRightJoin = false
	node := &ast.DeleteStmt{
		Where:        g.WhereClauseAst(gCtx, 2),
		IgnoreErr:    true,
		IsMultiTable: true,
		BeforeFrom:   true,
		TableRefs:    &ast.TableRefsClause{TableRefs: &ast.Join{}},
		Tables: &ast.DeleteTableList{
			Tables: []*ast.TableName{{Name: currTable.Name.ToModel()}},
		},
	}
	g.genResultSetNode(node.TableRefs.TableRefs, gCtx)
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
