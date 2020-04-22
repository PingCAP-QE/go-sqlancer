package executor

import (
	"github.com/juju/errors"

	"github.com/chaos-mesh/private-wreck-it/pkg/types"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

// GenerateDDLCreateTable rand create table statement
func (e *Executor) GenerateDDLCreateTable(colTypes []string) (*types.SQL, error) {
	tree := createTableStmt()

	stmt, table, err := e.walkDDLCreateTable(tree, colTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &types.SQL{
		SQLType:  types.SQLTypeDDLCreateTable,
		SQLTable: table,
		SQLStmt:  stmt,
	}, nil
}

// GenerateDDLCreateIndex rand create index statement
func (e *Executor) GenerateDDLCreateIndex() (*types.SQL, error) {
	tree := createIndexStmt()

	stmt, err := e.walkDDLCreateIndex(tree)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &types.SQL{
		SQLType: types.SQLTypeDDLCreateIndex,
		SQLStmt: stmt,
	}, nil
}

// GenerateDMLInsertByTable rand insert statement for specific table
func (e *Executor) GenerateDMLInsertByTable(table string) (*types.SQL, error) {
	tree := insertStmt()
	stmt, err := e.walkInsertStmtForTable(tree, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &types.SQL{
		SQLType:  types.SQLTypeDMLInsert,
		SQLStmt:  stmt,
		SQLTable: table,
	}, nil
}

func makeConstraintPrimaryKey(node *ast.CreateTableStmt, column string) {
	for _, constraint := range node.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			constraint.Keys = append(constraint.Keys, &ast.IndexPartSpecification{
				Column: &ast.ColumnName{
					Name: model.NewCIStr(column),
				},
			})
			return
		}
	}
	node.Constraints = append(node.Constraints, &ast.Constraint{
		Tp: ast.ConstraintPrimaryKey,
		Keys: []*ast.IndexPartSpecification{
			{
				Column: &ast.ColumnName{
					Name: model.NewCIStr(column),
				},
			},
		},
	})
}
