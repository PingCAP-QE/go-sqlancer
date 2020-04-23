package executor

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"

	"github.com/chaos-mesh/go-sqlancer/pkg/util"
)

func createTableStmt() *ast.CreateTableStmt {
	createTableNode := ast.CreateTableStmt{
		Table:       &ast.TableName{},
		Cols:        []*ast.ColumnDef{},
		Constraints: []*ast.Constraint{},
		Options:     []*ast.TableOption{},
	}
	// TODO: config for enable partition
	// partitionStmt is disabled
	// createTableNode.Partition = s.partitionStmt()

	return &createTableNode
}

func createIndexStmt() *ast.CreateIndexStmt {
	var indexType model.IndexType
	switch util.Rd(2) {
	case 0:
		indexType = model.IndexTypeBtree
	default:
		indexType = model.IndexTypeHash
	}

	node := ast.CreateIndexStmt{
		Table:                   &ast.TableName{},
		IndexPartSpecifications: []*ast.IndexPartSpecification{},
		IndexOption: &ast.IndexOption{
			Tp: indexType,
		},
	}
	return &node
}

func insertStmt() *ast.InsertStmt {
	insertStmtNode := ast.InsertStmt{
		Table: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{},
			},
		},
		Lists:   [][]ast.ExprNode{},
		Columns: []*ast.ColumnName{},
	}
	return &insertStmtNode
}
