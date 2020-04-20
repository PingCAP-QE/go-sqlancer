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
	"math/rand"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"

	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/stateflow"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/types"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/util"
	// _ "github.com/pingcap/tidb/types/parser_driver"
)

// SQLSmith defines SQLSmith struct
type SQLSmith struct {
	depth         int
	maxDepth      int
	Rand          *rand.Rand
	Databases     map[string]*types.Database
	subTableIndex int
	Node          ast.Node
	currDB        string
	debug         bool
	stable        bool
	hint          bool
}

// New create SQLSmith instance
func New() *SQLSmith {
	return new()
}

func new() *SQLSmith {
	return &SQLSmith{
		Rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		Databases: make(map[string]*types.Database),
	}
}

// Debug turn on debug mode
func (s *SQLSmith) Debug() {
	s.debug = true
}

// SetDB set current database
func (s *SQLSmith) SetDB(db string) {
	s.currDB = db
}

// GetCurrDBName returns current selected dbname
func (s *SQLSmith) GetCurrDBName() string {
	return s.currDB
}

// GetDB get current database without nil
func (s *SQLSmith) GetDB(db string) *types.Database {
	if db, ok := s.Databases[db]; ok {
		return db
	}
	return &types.Database{
		Name: db,
	}
}

// Stable set generated SQLs no rand
func (s *SQLSmith) Stable() {
	s.stable = true
}

// SetStable set stable to given value
func (s *SQLSmith) SetStable(stable bool) {
	s.stable = stable
}

// Hint ...
func (s *SQLSmith) Hint() bool {
	return s.hint
}

// SetHint ...
func (s *SQLSmith) SetHint(hint bool) {
	s.hint = hint
}

// WalkRaw will walk the tree and fillin tables and columns data
func (s *SQLSmith) WalkRaw(tree ast.Node) (string, *types.Table, error) {
	//node, table, err :=
	table, err := stateflow.New(s.GetDB(s.currDB), s.stable).WalkTreeRaw(tree)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	s.debugPrintf("node AST %+v\n", tree)
	sql, err := util.BufferOut(tree)
	return sql, table, errors.Trace(err)
}

// Walk will walk the tree and fillin tables and columns data
func (s *SQLSmith) Walk(tree ast.Node) (string, string, error) {
	sql, table, err := s.WalkRaw(tree)
	name := ""
	if table != nil {
		name = table.Table
	}
	return sql, name, err
}
