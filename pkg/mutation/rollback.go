package mutation

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types/mutasql"
)

type Rollback struct {
}

func (m *Rollback) Condition(*mutasql.TestCase) bool {

}

func (m *Rollback) Mutate(*mutasql.TestCase) (mutasql.TestCase, error) {

}
