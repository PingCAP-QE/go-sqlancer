package generator

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/generator/hint"
	"github.com/chaos-mesh/go-sqlancer/pkg/generator/operator"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	parser_types "github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

type Generator struct {
	Tables           []types.Table
	allowColumnTypes []string

	Config
}

type GenCtx struct {
	IsInExprIndex bool
	tmpTableIndex int
	tmpColIndex   int
	ResultTables  []types.Table
	TableAlias    map[string]string
}

func NewGenCtx(isInExprIndex bool) *GenCtx {
	return &GenCtx{
		IsInExprIndex: isInExprIndex,
		tmpTableIndex: 0,
		tmpColIndex:   0,
		ResultTables:  make([]types.Table, 0),
		TableAlias:    make(map[string]string),
	}
}

func (g *GenCtx) resetTmpTable() {
	g.tmpTableIndex = 0
}

func (g *GenCtx) getTmpTable() string {
	g.tmpTableIndex++
	return fmt.Sprintf("tmp%d", g.tmpTableIndex)
}

func (g *GenCtx) resetTmpColumn() {
	g.tmpColIndex = 0
}

func (g *GenCtx) getTmpColumn() string {
	g.tmpColIndex++
	return fmt.Sprintf("col_%d", g.tmpColIndex)
}

func (g *Generator) SelectStmtAst(depth int, usedTables []types.Table) (ast.SelectStmt, *GenCtx, error) {
	selectStmtNode := ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{},
		},
	}
	genCtx := NewGenCtx(false)

	selectStmtNode.Where = g.WhereClauseAst(genCtx, depth, usedTables)

	selectStmtNode.From = &ast.TableRefsClause{
		TableRefs: &ast.Join{
			Left:  &ast.TableName{},
			Right: &ast.TableName{},
		},
	}
	selectStmtNode.TableHints = g.tableHintsExpr(usedTables)
	return selectStmtNode, genCtx, nil
}

func (g *Generator) WhereClauseAst(ctx *GenCtx, depth int, usedTables []types.Table) ast.ExprNode {
	// TODO: support single operation like NOT
	// TODO: support func
	// TODO: support subquery
	// TODO: more ops
	// TODO: support single value AS bool
	pthese := ast.ParenthesesExpr{}
	switch Rd(4) {
	case 0:
		g.makeUnaryOp(ctx, &pthese, depth, usedTables)
	default:
		g.makeBinaryOp(ctx, &pthese, depth, usedTables)
	}
	return &pthese
}

// change ParenthesesExpr to more extensive
func (g *Generator) makeBinaryOp(ctx *GenCtx, e *ast.ParenthesesExpr, depth int, usedTables []types.Table) {
	node := ast.BinaryOperationExpr{}
	e.Expr = &node
	if depth > 0 {
		// f := operator.LogicOps[Rd(len(operator.LogicOps))]
		f := operator.LogicOps.Rand()
		switch t := f.(type) {
		case *types.Op:
			node.Op = t.GetOpcode()
		case *types.Fn:
			panic("not implement binary functions")
		}
		node.L = g.WhereClauseAst(ctx, depth-1, usedTables)
		node.R = g.WhereClauseAst(ctx, Rd(depth), usedTables)
	} else {
		f := operator.BinaryOps.Rand()
		switch t := f.(type) {
		case *types.Op:
			node.Op = t.GetOpcode()
		case *types.Fn:
			panic("not implement binary functions")
		}
		argType := 0
		acceptType := types.AnyArg
		if ctx.IsInExprIndex { // avoid string ops in CREATE INDEX stmt
			acceptType &^= types.DatetimeArg | types.StringArg
		}
		if Rd(3) > 0 {
			node.L = g.columnExpr(usedTables, acceptType)
			argType = TransMysqlType(node.L.GetType())
		} else {
			node.L = g.constValueExpr(acceptType)
			argType = TransMysqlType(node.L.GetType())
		}
		acceptType = f.GetAcceptType(0, argType)
		if ctx.IsInExprIndex {
			acceptType &^= types.StringArg // clear string type from accept types
		}
		if Rd(3) > 0 {
			node.R = g.columnExpr(usedTables, acceptType)
		} else {
			node.R = g.constValueExpr(acceptType)
		}
	}
}

func (g *Generator) makeUnaryOp(ctx *GenCtx, e *ast.ParenthesesExpr, depth int, usedTables []types.Table) {
	node := ast.UnaryOperationExpr{}
	e.Expr = &node
	if depth > 0 {
		switch Rd(1) {
		default:
			node.Op = opcode.Not
			node.V = g.WhereClauseAst(ctx, depth-1, usedTables)
		}
	} else {
		switch Rd(1) {
		default:
			node.Op = opcode.Not
			arg := types.AnyArg
			if ctx.IsInExprIndex {
				arg &^= types.StringArg | types.DatetimeArg
			}
			// no need to check params number
			if Rd(3) > 0 {
				node.V = g.columnExpr(usedTables, arg)
			} else {
				node.V = g.constValueExpr(arg)
			}
		}
	}
}

// TODO: important! resolve random when a kind was banned
func (g *Generator) constValueExpr(arg int) ast.ValueExpr {
	switch x := Rd(13); x {
	case 6, 7, 8, 9, 10:
		if arg&types.FloatArg != 0 {
			switch y := Rd(6); y {
			case 0, 1:
				return ast.NewValueExpr(float64(0), "", "")
			case 2:
				return ast.NewValueExpr(float64(1.0), "", "")
			case 3:
				return ast.NewValueExpr(float64(-1.0), "", "")
			default:
				return ast.NewValueExpr(RdFloat64(), "", "")
			}
		}
		fallthrough
	//case 7:
	//	if arg&DatetimeArg != 0 {
	//		t := tidb_types.NewTime(tidb_types.FromGoTime(RdDate()), mysql.TypeDatetime, 0)
	//		n := ast.NewValueExpr(t, "", "")
	//		return n
	//	}
	//	fallthrough
	//case 6:
	//	if arg&DatetimeArg != 0 {
	//		t := tidb_types.NewTime(tidb_types.FromGoTime(RdTimestamp()), mysql.TypeTimestamp, int8(Rd(7)))
	//		n := ast.NewValueExpr(t, "", "")
	//		return n
	//	}
	//	fallthrough
	case 3, 4, 5:
		if arg&types.IntArg != 0 {
			switch y := Rd(6); y {
			case 0, 1:
				return ast.NewValueExpr(0, "", "")
			case 2:
				return ast.NewValueExpr(1, "", "")
			case 3:
				return ast.NewValueExpr(-1, "", "")
			default:
				return ast.NewValueExpr(RdInt64(), "", "")
			}
		}
		fallthrough
	case 0, 1, 2:
		if arg&types.StringArg != 0 {
			switch y := Rd(3); y {
			case 0, 1:
				return ast.NewValueExpr(RdString(Rd(10)), "", "")
			default:
				return ast.NewValueExpr("", "", "")
			}
		} else if arg&types.NumberLikeStringArg != 0 { // TODO: fix number like string is always before datetime
			return ast.NewValueExpr(fmt.Sprintf("%d", Rd(1000)), "", "")
		} else if arg&types.DatetimeAsStringArg != 0 {
			return ast.NewValueExpr(RdTimestamp().Format("2006-01-02 15:04:05"), "", "")
		}
		fallthrough
	default:
		// NULL?
		if arg&types.NullArg == 0 {
			// generate again
			return g.constValueExpr(arg)
		}
		return ast.NewValueExpr(nil, "", "")
	}
}

func (g *Generator) columnExpr(usedTables []types.Table, arg int) *ast.ColumnNameExpr {
	randTable := usedTables[Rd(len(usedTables))]
	tempCols := make([]types.Column, 0)
	for i := range randTable.Columns {
		if TransStringType(randTable.Columns[i].Type)&arg != 0 {
			tempCols = append(tempCols, randTable.Columns[i])
		}
	}
	if len(tempCols) == 0 {
		panic(fmt.Sprintf("no valid column as arg %d table %s", arg, randTable.Name))
	}
	randColumn := tempCols[Rd(len(tempCols))]
	colName, typeStr := randColumn.Name, randColumn.Type
	col := new(ast.ColumnNameExpr)
	col.Name = &ast.ColumnName{
		Table: randTable.TmpTableName().ToModel(),
		Name:  colName.ToModel(),
	}
	col.Type = parser_types.FieldType{}
	col.SetType(tidb_types.NewFieldType(TransToMysqlType(TransStringType(typeStr))))
	return col
}

// walk on select stmt
func (g *Generator) SelectStmt(node *ast.SelectStmt, genCtx *GenCtx, usedTables []types.Table,
	pivotRows map[string]*connection.QueryItem) (string, []types.Column, map[string]*connection.QueryItem, error) {
	g.walkResultSetNode(node.From.TableRefs, usedTables)
	g.RectifyResultSetNode(node.From.TableRefs, genCtx, usedTables, pivotRows)
	// if node.From.TableRefs.Right == nil && node.From.TableRefs.Left != nil {
	// 	table = s.walkResultSetNode(node.From.TableRefs.Left)
	// 	s.walkSelectStmtColumns(node, table, false)
	// 	table.AddToInnerTables(table)
	// } else if node.From.TableRefs.Right != nil && node.From.TableRefs.Left != nil {
	// 	lTable := s.walkResultSetNode(node.From.TableRefs.Left)
	// 	rTable := s.walkResultSetNode(node.From.TableRefs.Right)

	// 	mergeTable, _ := s.mergeTable(lTable, rTable)
	// 	if node.From.TableRefs.On != nil {
	// 		s.walkOnStmt(node.From.TableRefs.On, lTable, rTable)
	// 	}
	// 	table = mergeTable

	// 	s.walkSelectStmtColumns(node, table, true)
	// }
	columnInfos, updatedPivotRows := g.walkResultFields(node, genCtx, genCtx.ResultTables, usedTables, pivotRows)
	// s.walkOrderByClause(node.OrderBy, table)
	node.Where = g.RectifyCondition(node.Where, genCtx, genCtx.ResultTables, pivotRows)
	// s.walkExprNode(node.Where, table, nil)
	sql, err := BufferOut(node)
	return sql, columnInfos, updatedPivotRows, err
}

func evaluateRow(e ast.Node, genCtx *GenCtx, usedTables []types.Table, pivotRows map[string]interface{}) parser_driver.ValueExpr {
	switch t := e.(type) {
	case *ast.ParenthesesExpr:
		return evaluateRow(t.Expr, genCtx, usedTables, pivotRows)
	case *ast.BinaryOperationExpr:
		res, err := operator.BinaryOps.Eval(opcode.Ops[t.Op], evaluateRow(t.L, genCtx, usedTables, pivotRows), evaluateRow(t.R, genCtx, usedTables, pivotRows))
		if err != nil {
			panic(fmt.Sprintf("error occurred on eval: %+v", err))
		}
		return res
	case *ast.UnaryOperationExpr:
		res, err := operator.UnaryOps.Eval(opcode.Ops[t.Op], evaluateRow(t.V, genCtx, usedTables, pivotRows))
		if err != nil {
			panic(fmt.Sprintf("error occurred on eval: %+v", err))
		}
		return res
	case *ast.IsNullExpr:
		subResult := evaluateRow(t.Expr, genCtx, usedTables, pivotRows)
		c := ConvertToBoolOrNull(subResult)
		r := parser_driver.ValueExpr{}
		r.SetInt64(0)
		if c == -1 {
			r.SetInt64(1)
		}
		return r
	case *ast.ColumnNameExpr:
		for key, value := range pivotRows {
			originTableName := t.Name.Table.L
			for k, v := range genCtx.TableAlias {
				if v == originTableName {
					originTableName = k
					break
				}
			}
			originColumnName := t.Name.Name.L
			if key == fmt.Sprintf("%s.%s", originTableName, originColumnName) {
				v := parser_driver.ValueExpr{}
				v.SetValue(value)
				if tmpTable, ok := genCtx.TableAlias[t.Name.Table.L]; ok {
					t.Name.Table = model.NewCIStr(tmpTable)
				}
				return v
			}
		}
		panic(fmt.Sprintf("no such col %s in table %s", t.Name, t.Name.Table))
	case ast.ValueExpr:
		v := parser_driver.ValueExpr{}
		v.SetValue(t.GetValue())
		v.SetType(t.GetType())
		return v
	}

	if e == nil {
		return trueValueExpr()
	}

	panic("not reachable")
	v := parser_driver.ValueExpr{}
	v.SetNull()
	return v
}

func Evaluate(whereClause ast.Node, genCtx *GenCtx, usedTables []types.Table, pivotRows map[string]*connection.QueryItem) parser_driver.ValueExpr {
	row := map[string]interface{}{}
	for key, value := range pivotRows {
		row[key], _ = getTypedValue(value)
	}
	return evaluateRow(whereClause, genCtx, usedTables, row)
}

func trueValueExpr() parser_driver.ValueExpr {
	d := tidb_types.Datum{}
	d.SetInt64(1)
	return parser_driver.ValueExpr{
		TexprNode: ast.TexprNode{},
		Datum:     d,
	}
}

func getTypedValue(it *connection.QueryItem) (interface{}, byte) {
	if it.Null {
		return nil, mysql.TypeNull
	}
	switch it.ValType.DatabaseTypeName() {
	case "VARCHAR", "TEXT", "CHAR":
		return it.ValString, mysql.TypeString
	case "INT", "BIGINT", "TINYINT":
		i, _ := strconv.ParseInt(it.ValString, 10, 64)
		return i, mysql.TypeLong
	case "TIMESTAMP", "DATE", "DATETIME":
		t, _ := time.Parse("2006-01-02 15:04:05", it.ValString)
		return tidb_types.NewTime(tidb_types.FromGoTime(t), mysql.TypeTimestamp, 6), mysql.TypeDatetime
	case "FLOAT", "DOUBLE", "DECIMAL":
		f, _ := strconv.ParseFloat(it.ValString, 64)
		return f, mysql.TypeDouble
	default:
		panic(fmt.Sprintf("unreachable type %s", it.ValType.DatabaseTypeName()))
	}
}

func (g *Generator) RectifyResultSetNode(node ast.ResultSetNode, genCtx *GenCtx, usedTables []types.Table,
	pivotRows map[string]*connection.QueryItem) {
	switch node := node.(type) {
	case *ast.Join:
		{
			g.RectifyJoin(node, genCtx, usedTables, pivotRows)
		}
	default:
		panic("unreachable")
	}
}

func (g *Generator) RectifyJoin(node *ast.Join, genCtx *GenCtx, usedTables []types.Table,
	pivotRows map[string]*connection.QueryItem) {
	if node.Right == nil {
		if node, ok := node.Left.(*ast.TableSource); ok {
			if tn, ok := node.Source.(*ast.TableName); ok {
				for _, table := range usedTables {
					if table.Name.EqModel(tn.Name) {
						genCtx.ResultTables = append(genCtx.ResultTables, table.Clone())
						return
					}
				}
			}
		}
		panic("unreachable")
	}

	if right, ok := node.Right.(*ast.TableSource); ok {
		var (
			leftTables []types.Table
			rightTable types.Table
		)
		switch node := node.Left.(type) {
		case *ast.Join:
			g.RectifyJoin(node, genCtx, usedTables, pivotRows)
			leftTables = genCtx.ResultTables
		case *ast.TableSource:
			{
				if tn, ok := node.Source.(*ast.TableName); ok {
					for _, table := range usedTables {
						if table.Name.EqModel(tn.Name) {
							tmpTable := genCtx.getTmpTable()
							node.AsName = model.NewCIStr(tmpTable)
							leftTables = []types.Table{table.Rename(tmpTable)}
							genCtx.TableAlias[table.Name.String()] = tmpTable
							break
						}
					}
				}
			}
		default:
			panic("unreachable")
		}
		for _, table := range usedTables {
			if table.Name.EqModel(right.Source.(*ast.TableName).Name) {
				tmpTable := genCtx.getTmpTable()
				right.AsName = model.NewCIStr(tmpTable)
				rightTable = table.Rename(tmpTable)
				genCtx.TableAlias[table.Name.String()] = tmpTable
			}
		}
		allTables := append(leftTables, rightTable)
		genCtx.ResultTables = allTables
		node.On = &ast.OnCondition{}
		node.On.Expr = g.WhereClauseAst(genCtx, 0, allTables)
		node.On.Expr = g.RectifyCondition(node.On.Expr, genCtx, allTables, pivotRows)
		return
	}

	panic("unreachable")
}

func (g *Generator) RectifyCondition(node ast.ExprNode, genCtx *GenCtx, usedTables []types.Table, pivotRows map[string]*connection.QueryItem) ast.ExprNode {
	out := Evaluate(node, genCtx, usedTables, pivotRows)
	pthese := ast.ParenthesesExpr{}
	pthese.Expr = node
	switch out.Kind() {
	case tidb_types.KindNull:
		node = &ast.IsNullExpr{
			Expr: &pthese,
			Not:  false,
		}
	default:
		// make it true
		zero := parser_driver.ValueExpr{}
		zero.SetInt64(0)
		res, _ := out.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &zero.Datum)
		if res == 0 {
			node = &ast.UnaryOperationExpr{
				Op: opcode.Not,
				V:  &pthese,
			}
		}
	}
	return node
}

func (g *Generator) walkResultFields(node *ast.SelectStmt, genCtx *GenCtx, resultTables []types.Table, usedTables []types.Table,
	pivotRows map[string]*connection.QueryItem) ([]types.Column, map[string]*connection.QueryItem) {
	columns := make([]types.Column, 0)
	rows := make(map[string]*connection.QueryItem)
	for _, table := range resultTables {
		for _, column := range table.Columns {
			asname := genCtx.getTmpColumn()
			selectField := ast.SelectField{
				Expr:   column.ToModel(),
				AsName: model.NewCIStr(asname),
			}
			node.Fields.Fields = append(node.Fields.Fields, &selectField)
			col := column.Clone()
			col.AliasName = types.CIStr(asname)
			columns = append(columns, col)
			rows[asname] = pivotRows[column.String()]
		}
	}
	// for _, userTable := range usedTables {
	// 	for _, resultTable := range resultTables {
	// 		if userTable.Name == resultTable.Name {
	// 			tableMap[userTable.Name.String()] = resultTable.TmpTableName().String()
	// 		}
	// 	}
	// }
	return columns, rows
}

func (g *Generator) walkResultSetNode(node *ast.Join, usedTables []types.Table) {
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
		switch Rd(3) {
		case 0:
			tp = ast.RightJoin
		case 1:
			tp = ast.LeftJoin
		default:
			tp = ast.CrossJoin
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

func (g *Generator) CollectColumnNames(node ast.Node) []ast.ColumnName {
	collector := columnNameVisitor{
		Columns: make(map[string]ast.ColumnName),
	}
	node.Accept(&collector)
	var columns columnNames

	for _, column := range collector.Columns {
		columns = append(columns, column)
	}
	sort.Sort(columns)
	return columns
}

type columnNames []ast.ColumnName

func (c columnNames) Len() int {
	return len(c)
}

func (c columnNames) Less(i, j int) bool {
	return c[i].String() < c[j].String()
}

func (c columnNames) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type columnNameVisitor struct {
	Columns map[string]ast.ColumnName
}

func (v *columnNameVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *ast.ColumnName:
		if _, ok := v.Columns[n.String()]; !ok {
			v.Columns[n.String()] = *n
		}
	}
	return in, false
}

func (v *columnNameVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
