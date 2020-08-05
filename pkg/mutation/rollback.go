package mutation

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/generator"
	"github.com/chaos-mesh/go-sqlancer/pkg/types/mutasql"
	"github.com/pingcap/parser/ast"
)

type Rollback struct {
}

func (m *Rollback) Condition(tc *mutasql.TestCase) bool {
	return tc.Mutable && len(tc.GetAllTables()) > 0
}

func (m *Rollback) Mutate(tc *mutasql.TestCase, g *generator.Generator) (mutasql.TestCase, error) {
	mutated := tc.Clone()

	startTxnNode := &ast.BeginStmt{}
	rollbackTxnNode := &ast.RollbackStmt{}

	mutated.AfterInsert = append(mutated.AfterInsert, startTxnNode)
	mutated.CleanUp = append(mutated.CleanUp, rollbackTxnNode)

	return mutated, nil
}
