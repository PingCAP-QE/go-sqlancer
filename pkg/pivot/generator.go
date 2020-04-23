package pivot

import (
	"fmt"
	"strconv"
	"time"

	"github.com/chaos-mesh/private-wreck-it/pkg/connection"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

type Table struct {
	Name    model.CIStr
	Columns [][3]string
	Indexes []model.CIStr
}

type TableColumn struct {
	Table string
	Name  string
}

type Generator struct {
	Tables []Table
}

func (g *Generator) selectStmtAst(depth int, usedTables []Table) (ast.SelectStmt, error) {
	selectStmtNode := ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{},
		},
	}

	selectStmtNode.Where = g.whereClauseAst(depth, usedTables)

	selectStmtNode.From = &ast.TableRefsClause{
		TableRefs: &ast.Join{
			Left:  &ast.TableName{},
			Right: &ast.TableName{},
		},
	}
	return selectStmtNode, nil
}

func (g *Generator) whereClauseAst(depth int, usedTables []Table) ast.ExprNode {
	// TODO: support single operation like NOT
	// TODO: support func
	// TODO: support subquery
	// TODO: more ops
	// TODO: support single value AS bool
	pthese := ast.ParenthesesExpr{}
	switch Rd(4) {
	case 0:
		g.makeUnaryOp(&pthese, depth, usedTables)
	default:
		g.makeBinaryOp(&pthese, depth, usedTables)
	}
	return &pthese
}

// change ParenthesesExpr to more extensive
func (g *Generator) makeBinaryOp(e *ast.ParenthesesExpr, depth int, usedTables []Table) {
	node := ast.BinaryOperationExpr{}
	e.Expr = &node
	if depth > 0 {
		r := Rd(3)
		switch r {
		case 0:
			node.Op = opcode.LogicXor
			node.L = g.whereClauseAst(depth-1, usedTables)
			node.R = g.whereClauseAst(Rd(depth), usedTables)
		case 1:
			node.Op = opcode.LogicOr
			node.L = g.whereClauseAst(depth-1, usedTables)
			node.R = g.whereClauseAst(Rd(depth), usedTables)
		default:
			node.Op = opcode.LogicAnd
			node.L = g.whereClauseAst(depth-1, usedTables)
			node.R = g.whereClauseAst(Rd(depth), usedTables)
		}
	} else {
		var f Function
		switch Rd(9) {
		case 0:
			node.Op = opcode.GT
			f = Gt
		case 1:
			node.Op = opcode.LT
			f = Lt
		case 2:
			node.Op = opcode.NE
			f = Ne
		case 3:
			node.Op = opcode.LE
			f = Le
		case 4:
			node.Op = opcode.GE
			f = Ge
		case 5:
			node.Op = opcode.EQ
			f = Eq
		case 6:
			node.Op = opcode.LogicXor
			f = LogicAnd
		case 7:
			node.Op = opcode.LogicOr
			f = LogicOr
		default:
			node.Op = opcode.LogicAnd
			f = LogicXor
		}
		argType := 0
		if Rd(3) > 0 {
			node.L = g.columnExpr(usedTables, AnyArg)
			argType = TransMysqlType(node.L.GetType())
		} else {
			node.L = g.constValueExpr(AnyArg)
			argType = TransMysqlType(node.L.GetType())
		}
		acceptType := f.AcceptType[0][argType]
		if Rd(3) > 0 {
			node.R = g.columnExpr(usedTables, acceptType)
		} else {
			node.R = g.constValueExpr(acceptType)
		}
	}
}

func (g *Generator) makeUnaryOp(e *ast.ParenthesesExpr, depth int, usedTables []Table) {
	node := ast.UnaryOperationExpr{}
	e.Expr = &node
	if depth > 0 {
		switch Rd(1) {
		default:
			node.Op = opcode.Not
			node.V = g.whereClauseAst(depth-1, usedTables)
		}
	} else {
		switch Rd(1) {
		default:
			node.Op = opcode.Not
			// no need to check params number
			if Rd(3) > 0 {
				node.V = g.columnExpr(usedTables, AnyArg)
			} else {
				node.V = g.constValueExpr(AnyArg)
			}
		}
	}
}

// TODO: important! resolve random when a kind was banned
func (g *Generator) constValueExpr(arg int) ast.ValueExpr {
	switch x := Rd(13); x {
	case 6, 7, 8, 9, 10:
		if arg&FloatArg != 0 {
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
		if arg&IntArg != 0 {
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
		if arg&StringArg != 0 {
			switch y := Rd(3); y {
			case 0, 1:
				return ast.NewValueExpr(RdString(Rd(10)), "", "")
			default:
				return ast.NewValueExpr("", "", "")
			}
		} else if arg&DatetimeAsStringArg != 0 {
			return ast.NewValueExpr(RdTimestamp().Format("2006-01-02 15:04:05"), "", "")
		}
		fallthrough
	default:
		// NULL?
		if arg&NullArg == 0 {
			// generate again
			return g.constValueExpr(arg)
		}
		return ast.NewValueExpr(nil, "", "")
	}
}

func (g *Generator) columnExpr(usedTables []Table, arg int) *ast.ColumnNameExpr {
	randTable := usedTables[Rd(len(usedTables))]
	tempCols := make([][3]string, 0)
	for i := range randTable.Columns {
		if TransStringType(randTable.Columns[i][1])&arg != 0 {
			tempCols = append(tempCols, randTable.Columns[i])
		}
	}
	if len(tempCols) == 0 {
		panic(fmt.Sprintf("no valid column as arg %d", arg))
	}
	randColumn := tempCols[Rd(len(tempCols))]
	colName, typeStr := randColumn[0], randColumn[1]
	col := new(ast.ColumnNameExpr)
	col.Name = &ast.ColumnName{
		Table: randTable.Name,
		Name:  model.NewCIStr(colName),
	}
	col.Type = types.FieldType{}
	col.SetType(tidb_types.NewFieldType(TransToMysqlType(TransStringType(typeStr))))
	return col
}

// walk on select stmt
func (g *Generator) selectStmt(node *ast.SelectStmt, usedTables []Table, pivotRows map[TableColumn]*connection.QueryItem) (string, []TableColumn, error) {
	g.walkResultSetNode(node.From.TableRefs, usedTables)
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
	columnInfos := g.walkResultFields(node, usedTables)
	// s.walkOrderByClause(node.OrderBy, table)
	g.RectifyCondition(node, usedTables, pivotRows)
	// s.walkExprNode(node.Where, table, nil)
	sql, err := BufferOut(node)
	return sql, columnInfos, err
}

func evaluateRow(e ast.Node, usedTables []Table, pivotRows map[TableColumn]interface{}) parser_driver.ValueExpr {
	switch t := e.(type) {
	case *ast.ParenthesesExpr:
		return evaluateRow(t.Expr, usedTables, pivotRows)
	case *ast.BinaryOperationExpr:
		switch t.Op {
		case opcode.LogicXor:
			r, _ := LogicXor.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.LogicAnd:
			r, _ := LogicAnd.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.LogicOr:
			r, _ := LogicOr.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.GT:
			r, _ := Gt.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.GE:
			r, _ := Ge.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.LT:
			r, _ := Lt.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.LE:
			r, _ := Le.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.NE:
			r, _ := Ne.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		case opcode.EQ:
			r, _ := Eq.Eval(evaluateRow(t.L, usedTables, pivotRows), evaluateRow(t.R, usedTables, pivotRows))
			return r
		default:
			panic(fmt.Sprintf("no op implements %s %d", t.Op.String(), t.Op))
		}
	case *ast.UnaryOperationExpr:
		switch t.Op {
		case opcode.Not:
			r, _ := Not.Eval(evaluateRow(t.V, usedTables, pivotRows))
			return r
		}
	case *ast.IsNullExpr:
		subResult := evaluateRow(t.Expr, usedTables, pivotRows)
		c := ConvertToBoolOrNull(subResult)
		r := parser_driver.ValueExpr{}
		r.SetInt64(0)
		if c == -1 {
			r.SetInt64(1)
		}
		return r
	case *ast.ColumnNameExpr:
		for key, value := range pivotRows {
			if key.Table+"."+key.Name == t.Name.OrigColName() {
				v := parser_driver.ValueExpr{}
				v.SetValue(value)
				return v
			}
		}
		panic(fmt.Sprintf("no such col %s in table %s", t.Name, t.Name.Table))
	case ast.ValueExpr:
		v := parser_driver.ValueExpr{}
		v.SetValue(t.GetValue())
		v.SetType(t.GetType())
		return v
	case *parser_driver.ValueExpr: // is reachable?
		panic("not reachable")
	}

	if e == nil {
		return trueValueExpr()
	}

	panic("not reachable")
	v := parser_driver.ValueExpr{}
	v.SetNull()
	return v
}

func Evaluate(whereClause ast.Node, usedTables []Table, pivotRows map[TableColumn]*connection.QueryItem) parser_driver.ValueExpr {
	row := map[TableColumn]interface{}{}
	for key, value := range pivotRows {
		row[key], _ = getTypedValue(value)
	}
	return evaluateRow(whereClause, usedTables, row)
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

func (g *Generator) RectifyCondition(node *ast.SelectStmt, usedTables []Table, pivotRows map[TableColumn]*connection.QueryItem) {
	out := Evaluate(node.Where, usedTables, pivotRows)
	pthese := ast.ParenthesesExpr{}
	pthese.Expr = node.Where
	switch out.Kind() {
	case tidb_types.KindNull:
		node.Where = &ast.IsNullExpr{
			Expr: &pthese,
			Not:  false,
		}
	default:
		// make it true
		zero := parser_driver.ValueExpr{}
		zero.SetInt64(0)
		res, _ := out.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &zero.Datum)
		if res == 0 {
			node.Where = &ast.UnaryOperationExpr{
				Op: opcode.Not,
				V:  &pthese,
			}
		}
	}
}

func (g *Generator) walkResultFields(node *ast.SelectStmt, usedTables []Table) []TableColumn {
	columns := make([]TableColumn, 0)
	for _, table := range usedTables {
		for _, column := range table.Columns {
			selectField := ast.SelectField{
				Expr: &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Table: table.Name,
						Name:  model.NewCIStr(column[0]),
					},
				},
			}
			node.Fields.Fields = append(node.Fields.Fields, &selectField)
			columns = append(columns, TableColumn{table.Name.O, column[0]})
		}
	}
	return columns
}

func (g *Generator) walkResultSetNode(node *ast.Join, usedTables []Table) {
	l := len(usedTables)
	var left *ast.Join = node
	// TODO: it works, but need to refactory
	if l == 1 {
		ts := ast.TableSource{}
		tn := ast.TableName{}
		tn.Name = usedTables[0].Name
		ts.Source = &tn
		node.Left = &ts
		node.Right = nil
	}
	for i := l - 1; i >= 1; i-- {
		ts := ast.TableSource{}
		tn := ast.TableName{}
		tn.Name = usedTables[i].Name
		ts.Source = &tn
		if i > 1 {
			left.Right = &ts
			left.Left = &ast.Join{}
			left = left.Left.(*ast.Join)
		} else {
			left.Right = &ts
			ts2 := ast.TableSource{}
			tn2 := ast.TableName{}
			tn2.Name = usedTables[i-1].Name
			ts2.Source = &tn2
			left.Left = &ts2
		}
	}
}
