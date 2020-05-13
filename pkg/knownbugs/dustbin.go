package knownbugs

import (
	"fmt"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
)

type Dustbin struct {
	PivotRows map[string]*connection.QueryItem

	Stmts []ast.Node

	// may add some configs and contexts
}
type KnownBug = func(*Dustbin) bool

var (
	Bugs map[string]KnownBug = make(map[string]KnownBug)
)

func NewDustbin(stmts []ast.Node, pivots map[string]*connection.QueryItem) Dustbin {
	return Dustbin{Stmts: stmts, PivotRows: pivots}
}

func (d *Dustbin) IsKnownBug() bool {
	if len(d.Stmts) == 0 {
		panic(errors.New("empty statements in dustbin"))
	}
	for k, b := range Bugs {
		if b(d) {
			log.L().Info(fmt.Sprintf("this bug has been found: %s", k))
			return true
		}
	}
	return false
}

func init() {
	Bugs["issue16788"] = issue16788
}
