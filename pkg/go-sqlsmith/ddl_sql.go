// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlsmith

import (
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	parserTypes "github.com/pingcap/parser/types"

	"github.com/chaos-mesh/private-wreck-it/pkg/generator"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/stateflow"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/types"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/util"
)

// CreateTableStmt create table
func (s *SQLSmith) CreateTableStmt(colTypes []string) (string, string, error) {
	tree := s.createTableStmt().(*ast.CreateTableStmt)

	table := fmt.Sprintf("%s_%s", "table", strings.Join(colTypes, "_"))

	sf := stateflow.New(s.GetDB(s.currDB), s.stable)
	idFieldType := parserTypes.NewFieldType(util.Type2Tp("int"))
	idFieldType.Flen = dataType2Len("int")
	idCol := &ast.ColumnDef{
		Name:    &ast.ColumnName{Name: model.NewCIStr(fmt.Sprintf("id"))},
		Tp:      idFieldType,
		Options: []*ast.ColumnOption{{Tp: ast.ColumnOptionAutoIncrement}},
	}
	tree.Cols = append(tree.Cols, idCol)
	sf.MakeConstraintPrimaryKey(tree, &types.Column{Column: "id"})

	tree.Table.Name = model.NewCIStr(table)
	for _, colType := range colTypes {
		fieldType := parserTypes.NewFieldType(util.Type2Tp(colType))
		fieldType.Flen = dataType2Len(colType)
		tree.Cols = append(tree.Cols, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: model.NewCIStr(fmt.Sprintf("col_%s", colType))},
			Tp:   fieldType,
		})
	}
	stmt, err := util.BufferOut(tree)
	return stmt, table, err
}

// AlterTableStmt alter table
func (s *SQLSmith) AlterTableStmt(opt *generator.DDLOptions) (string, error) {
	s.setOnlineOtherTables(opt)
	defer s.freeOnlineOtherTables()
	tree := s.alterTableStmt()
	stmt, _, err := s.Walk(tree)
	return stmt, err
}

// CreateIndexStmt create index
func (s *SQLSmith) CreateIndexStmt(opt *generator.DDLOptions) (string, error) {
	s.setOnlineOtherTables(opt)
	defer s.freeOnlineOtherTables()
	tree := s.createIndexStmt()
	stmt, _, err := s.Walk(tree)
	return stmt, err
}

func dataType2Len(t string) int {
	switch t {
	case "int":
		return 16
	case "varchar":
		return 1023
	case "timestamp":
		return 255
	case "datetime":
		return 255
	case "text":
		return 1023
	case "float":
		return 64
	}
	return 16
}
