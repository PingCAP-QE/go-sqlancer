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
	"github.com/pingcap/tidb/meta/autoid"
	tidbTypes "github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	intPartition = []int64{1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11}

	datetimePartition = []tidbTypes.CoreTime{
		tidbTypes.FromDate(1980, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(1990, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(2000, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(2010, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(2020, 1, 1, 0, 0, 0, 0),
	}
	timestampPartition = []tidbTypes.CoreTime{
		tidbTypes.FromDate(1980, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(1990, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(2000, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(2010, 1, 1, 0, 0, 0, 0),
		tidbTypes.FromDate(2020, 1, 1, 0, 0, 0, 0),
	}

	autoOptTypes = []ast.ColumnOptionType{ast.ColumnOptionAutoIncrement, ast.ColumnOptionAutoRandom}
)

func randAutoOpt() *ast.ColumnOption {
	opt := &ast.ColumnOption{Tp: autoOptTypes[util.Rd(len(autoOptTypes))]}
	if opt.Tp == ast.ColumnOptionAutoRandom {
		opt.AutoRandomBitLength = 1 + util.Rd(autoid.MaxAutoRandomBits)
	}
	return opt
}

func (e *Executor) walkDDLCreateTable(index int, node *ast.CreateTableStmt, colTypes []string) (string, string, error) {
	table := fmt.Sprintf("%s_%s", "table", strings.Join(colTypes, "_"))
	idColName := fmt.Sprintf("id_%d", index)

	idFieldType := parserTypes.NewFieldType(Type2Tp("bigint"))
	idFieldType.Flen = dataType2Len("bigint")
	idCol := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(idColName)},
		Tp:      idFieldType,
		Options: []*ast.ColumnOption{randAutoOpt()},
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
	if node.Partition != nil {
		if colType := e.walkPartition(index, node.Partition, colTypes); colType != "" {
			makeConstraintPrimaryKey(node, fmt.Sprintf("col_%s_%d", colType, index))
		} else {
			node.Partition = nil
		}
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
	node.Table.Name = table.Name.ToModel()
	node.IndexName = util.RdStringChar(5)
	for _, column := range table.Columns {
		name := column.Name.String()
		if column.Type == "text" {
			length := util.Rd(31) + 1
			name = fmt.Sprintf("%s(%d)", name, length)
		} else if column.Type == "varchar" {
			// length := 1
			// if column.DataLen > 1 {
			// 	maxLen := util.MinInt(column.DataLen, 32)
			// 	length = util.Rd(maxLen-1) + 1
			// }
			length := 1
			if column.Length > 1 {
				length = column.Length
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
	node.Table.TableRefs.Left.(*ast.TableName).Name = table.Name.ToModel()
	columns := e.walkColumns(&node.Columns, table)
	e.walkLists(&node.Lists, columns)
	return BufferOut(node)
}

func (e *Executor) walkColumns(columns *[]*ast.ColumnName, table *types.Table) []types.Column {
	var cols []types.Column
	for _, column := range table.Columns {
		if column.Name.HasPrefix("id_") || column.Name.EqString("id") {
			continue
		}
		*columns = append(*columns, &ast.ColumnName{
			Table: table.Name.ToModel(),
			Name:  column.Name.ToModel(),
		})
		cols = append(cols, column.Clone())
	}
	return cols
}

func (e *Executor) walkLists(lists *[][]ast.ExprNode, columns []types.Column) {
	count := int(util.RdRange(10, 20))
	for i := 0; i < count; i++ {
		*lists = append(*lists, randList(columns))
	}
	// *lists = append(*lists, randor0(columns)...)
}

func randor0(cols [][3]string) [][]ast.ExprNode {
	var (
		res     [][]ast.ExprNode
		zeroVal = ast.NewValueExpr(GenerateZeroDataItem(cols[0][1]), "", "")
		randVal = ast.NewValueExpr(GenerateDataItem(cols[0][1]), "", "")
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

func (e *Executor) walkPartition(index int, node *ast.PartitionOptions, colTypes []string) string {
	var availableCols []string
	for _, colType := range colTypes {
		switch colType {
		case "timestamp", "datetime", "int":
			availableCols = append(availableCols, colType)
		}
	}
	// no available cols to be partitioned, remove partition
	if len(availableCols) == 0 {
		return ""
	}

	colType := availableCols[util.Rd(len(availableCols))]
	node.Tp = model.PartitionTypeRange

	// set to int func
	var funcCallNode = new(ast.FuncCallExpr)
	switch colType {
	case "timestamp":
		funcCallNode.FnName = model.NewCIStr("UNIX_TIMESTAMP")
	case "datetime":
		funcCallNode.FnName = model.NewCIStr("TO_DAYS")
	// partitioned by ASCII function not work yet
	case "varchar", "text":
		funcCallNode.FnName = model.NewCIStr("ASCII")
	// partitioned by CEILING and FLOOR function not work yet
	// https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-functions.html#partitioning-limitations-ceiling-floor
	case "float":
		if util.Rd(2) == 0 {
			funcCallNode.FnName = model.NewCIStr("CEILING")
		} else {
			funcCallNode.FnName = model.NewCIStr("FLOOR")
		}
	}

	// partition by column
	partitionByFuncCall := funcCallNode
	if funcCallNode.FnName.String() == "" {
		node.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{
				Name: model.NewCIStr(fmt.Sprintf("col_%s_%d", colType, index)),
			},
		}
	} else {
		partitionByFuncCall.Args = []ast.ExprNode{
			&ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Name: model.NewCIStr(fmt.Sprintf("col_%s_%d", colType, index)),
				},
			},
		}
		node.Expr = partitionByFuncCall
	}

	// set partition definitions
	e.walkPartitionDefinitions(&node.Definitions, colType)
	return colType
}

func (e *Executor) walkPartitionDefinitions(definitions *[]*ast.PartitionDefinition, colType string) {
	switch colType {
	case "int", "float":
		e.walkPartitionDefinitionsInt(definitions)
	case "varchar", "text":
		e.walkPartitionDefinitionsString(definitions)
	case "datetime":
		e.walkPartitionDefinitionsDatetime(definitions)
	case "timestamp":
		e.walkPartitionDefinitionsTimestamp(definitions)
	}

	*definitions = append(*definitions, &ast.PartitionDefinition{
		Name: model.NewCIStr("pn"),
		Clause: &ast.PartitionDefinitionClauseLessThan{
			Exprs: []ast.ExprNode{
				&ast.MaxValueExpr{},
			},
		},
	})
}

func (e *Executor) walkPartitionDefinitionsInt(definitions *[]*ast.PartitionDefinition) {
	for i := 0; i < len(intPartition); i += int(util.RdRange(1, 3)) {
		val := driver.ValueExpr{}
		val.SetInt64(intPartition[i])
		*definitions = append(*definitions, &ast.PartitionDefinition{
			Name: model.NewCIStr(fmt.Sprintf("p%d", i)),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{
					&val,
				},
			},
		})
	}
}

func (e *Executor) walkPartitionDefinitionsString(definitions *[]*ast.PartitionDefinition) {
	for i := 0; i < 256; i += int(util.RdRange(1, 10)) {
		val := driver.ValueExpr{}
		val.SetInt64(int64(i))
		*definitions = append(*definitions, &ast.PartitionDefinition{
			Name: model.NewCIStr(fmt.Sprintf("p%d", i)),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{
					&ast.FuncCallExpr{
						FnName: model.NewCIStr("ASCII"),
						Args:   []ast.ExprNode{&val},
					},
				},
			},
		})
	}
}

func (e *Executor) walkPartitionDefinitionsDatetime(definitions *[]*ast.PartitionDefinition) {
	for i := 0; i < len(datetimePartition); i += int(util.RdRange(1, 3)) {
		val := driver.ValueExpr{}
		val.SetMysqlTime(tidbTypes.NewTime(datetimePartition[i], 0, 0))
		*definitions = append(*definitions, &ast.PartitionDefinition{
			Name: model.NewCIStr(fmt.Sprintf("p%d", i)),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{
					&ast.FuncCallExpr{
						FnName: model.NewCIStr("TO_DAYS"),
						Args:   []ast.ExprNode{&val},
					},
				},
			},
		})
	}
}

func (e *Executor) walkPartitionDefinitionsTimestamp(definitions *[]*ast.PartitionDefinition) {
	for i := 0; i < len(timestampPartition); i += int(util.RdRange(1, 3)) {
		val := driver.ValueExpr{}
		val.SetMysqlTime(tidbTypes.NewTime(timestampPartition[i], 0, 0))
		*definitions = append(*definitions, &ast.PartitionDefinition{
			Name: model.NewCIStr(fmt.Sprintf("p%d", i)),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{
					&ast.FuncCallExpr{
						FnName: model.NewCIStr("TO_DAYS"),
						Args:   []ast.ExprNode{&val},
					},
				},
			},
		})
	}
}

func randList(columns []types.Column) []ast.ExprNode {
	var list []ast.ExprNode
	for _, column := range columns {
		// GenerateEnumDataItem
		switch util.Rd(3) {
		case 0:
			if !column.Null {
				list = append(list, ast.NewValueExpr(GenerateEnumDataItem(column.Type), "", ""))
			} else {
				list = append(list, ast.NewValueExpr(nil, "", ""))
			}
		default:
			list = append(list, ast.NewValueExpr(GenerateEnumDataItem(column.Type), "", ""))
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
	case "bigint":
		return mysql.TypeLonglong
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
