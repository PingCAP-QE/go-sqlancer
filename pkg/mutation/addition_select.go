package mutation

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/generator"
	"github.com/chaos-mesh/go-sqlancer/pkg/types/mutasql"
)

type AdditionSelect struct {
}

func (m *AdditionSelect) Condition(tc *mutasql.TestCase) bool {
	return tc.Mutable && len(tc.GetAllTables()) > 0
}

func (m *AdditionSelect) Mutate(tc *mutasql.TestCase, g *generator.Generator) ([]*mutasql.TestCase, error) {
	mutated := tc.Clone()
	origin := tc.Clone()

	genCtx := generator.NewGenCtx(tc.GetAllTables(), nil)
	genCtx.IsPQSMode = false

	selectAST, _, _, _, err := g.SelectStmt(genCtx, 3)
	if err != nil {
		return nil, err
	}

	mutated.AfterInsert = append(mutated.AfterInsert, selectAST)

	return []*mutasql.TestCase{&origin, &mutated}, nil
}
