package executor

import (
	"fmt"
	"strings"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	parserTypes "github.com/pingcap/parser/types"
)

func (e *Executor) walkDDLCreateTable(index int, node *ast.CreateTableStmt, colTypes []string) (string, string, error) {
	table := fmt.Sprintf("%s_%s", "table", strings.Join(colTypes, "_"))
	idColName := fmt.Sprintf("id_%d", index)

	idFieldType := parserTypes.NewFieldType(Type2Tp("int"))
	idFieldType.Flen = dataType2Len("int")
	idCol := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(idColName)},
		Tp:      idFieldType,
		Options: []*ast.ColumnOption{{Tp: ast.ColumnOptionAutoIncrement}},
	}
	node.Cols = append(node.Cols, idCol)
	makeConstraintPrimaryKey(node, idColName)

	node.Table.Name = model.NewCIStr(table)
	for _, colType := range colTypes {
		fieldType := parserTypes.NewFieldType(Type2Tp(colType))
		fieldType.Flen = dataType2Len(colType)
		node.Cols = append(node.Cols, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: model.NewCIStr(fmt.Sprintf("col_%s_%d", colType, index))},
			Tp:   fieldType,
		})
	}
	sql, err := BufferOut(node)
	if err != nil {
		return "", "", err
	}
	return sql, table, errors.Trace(err)
}

func (e *Executor) walkDDLCreateIndex(node *ast.CreateIndexStmt) (string, error) {
	table := e.randTable()
	if table == nil {
		return "", errors.New("no table available")
	}
	node.Table.Name = model.NewCIStr(table.Table)
	node.IndexName = util.RdStringChar(5)
	for _, column := range table.Columns {
		name := column.Column
		if column.DataType == "text" {
			length := util.Rd(31) + 1
			name = fmt.Sprintf("%s(%d)", name, length)
		} else if column.DataType == "varchar" {
			length := 1
			if column.DataLen > 1 {
				maxLen := util.MinInt(column.DataLen, 32)
				length = util.Rd(maxLen-1) + 1
			}
			name = fmt.Sprintf("%s(%d)", name, length)
		}
		node.IndexPartSpecifications = append(node.IndexPartSpecifications,
			&ast.IndexPartSpecification{
				Column: &ast.ColumnName{
					Name: model.NewCIStr(name),
				},
			})
	}
	if len(node.IndexPartSpecifications) > 10 {
		node.IndexPartSpecifications = node.IndexPartSpecifications[:util.Rd(10)+1]
	}
	return BufferOut(node)
}

func (e *Executor) walkInsertStmtForTable(node *ast.InsertStmt, tableName string) (string, error) {
	table, ok := e.tables[tableName]
	if !ok {
		return "", errors.Errorf("table %s not exist", tableName)
	}
	node.Table.TableRefs.Left.(*ast.TableName).Name = model.NewCIStr(table.Table)
	columns := e.walkColumns(&node.Columns, table)
	e.walkLists(&node.Lists, columns)
	return BufferOut(node)
}

func (e *Executor) walkColumns(columns *[]*ast.ColumnName, table *types.Table) [][3]string {
	var cols [][3]string
	for _, column := range table.Columns {
		if strings.HasPrefix(column.Column, "id_") || column.Column == "id" {
			continue
		}
		*columns = append(*columns, &ast.ColumnName{
			Table: model.NewCIStr(table.Name),
			Name:  model.NewCIStr(column[0]),
		})
		cols = append(cols, column)
	}
	return cols
}

func (e *Executor) walkLists(lists *[][]ast.ExprNode, columns [][3]string) {
	var noIDColumns []*types.Column
	for _, column := range columns {
		if strings.HasPrefix(column.Column, "id_") || column.Column == "id" {
			continue
		}
		noIDColumns = append(noIDColumns, column)
	}
	count := util.RdRange(10, 20)
	for i := 0; i < count; i++ {
		*lists = append(*lists, randList(noIDColumns))
	}
	// *lists = append(*lists, randor0(columns)...)
}

func randor0(cols []*types.Column) [][]ast.ExprNode {
	var (
		res     [][]ast.ExprNode
		zeroVal = ast.NewValueExpr(GenerateZeroDataItem(cols[0]), "", "")
		randVal = ast.NewValueExpr(GenerateDataItem(cols[0]), "", "")
		nullVal = ast.NewValueExpr(nil, "", "")
	)

	if len(cols) == 1 {
		res = append(res, []ast.ExprNode{zeroVal})
		res = append(res, []ast.ExprNode{randVal})
		res = append(res, []ast.ExprNode{nullVal})
		return res
	}
	for _, sub := range randor0(cols[1:]) {
		res = append(res, append([]ast.ExprNode{zeroVal}, sub...))
		res = append(res, append([]ast.ExprNode{randVal}, sub...))
		res = append(res, append([]ast.ExprNode{nullVal}, sub...))
	}
	return res
}

func randList(columns []*types.Column) []ast.ExprNode {
	var list []ast.ExprNode
	for _, column := range columns {
		// GenerateEnumDataItem
		switch util.Rd(3) {
		case 0:
			if column.HasOption(ast.ColumnOptionNotNull) {
				list = append(list, ast.NewValueExpr(GenerateEnumDataItem(column), "", ""))
			} else {
				list = append(list, ast.NewValueExpr(nil, "", ""))
			}
		default:
			list = append(list, ast.NewValueExpr(GenerateEnumDataItem(column), "", ""))
		}
	}
	return list
}

func (e *Executor) randTable() *types.Table {
	var tables []*types.Table
	for _, t := range e.tables {
		tables = append(tables, t)
	}
	if len(tables) == 0 {
		return nil
	}
	return tables[util.Rd(len(tables))]
}

// Type2Tp conver type string to tp byte
// TODO: complete conversion map
func Type2Tp(t string) byte {
	switch t {
	case "int":
		return mysql.TypeLong
	case "varchar":
		return mysql.TypeVarchar
	case "timestamp":
		return mysql.TypeTimestamp
	case "datetime":
		return mysql.TypeDatetime
	case "text":
		return mysql.TypeBlob
	case "float":
		return mysql.TypeFloat
	}
	return mysql.TypeNull
}
