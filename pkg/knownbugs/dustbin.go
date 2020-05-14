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
type NodeCb = func(ast.Node) (ast.Node, bool)
type Visitor struct {
	enterFunc NodeCb
	leaveFunc NodeCb
}

func NewVisitor() Visitor {
	return Visitor{
		enterFunc: emptyNodeCb,
		leaveFunc: emptyNodeCb,
	}
}

var (
	Bugs        map[string]KnownBug = make(map[string]KnownBug)
	emptyNodeCb NodeCb              = func(in ast.Node) (ast.Node, bool) {
		return in, true
	}
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

func (v *Visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return v.enterFunc(in)
}

func (v *Visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return v.leaveFunc(in)
}

func (v *Visitor) SetEnter(e NodeCb) {
	v.enterFunc = e
}

func (v *Visitor) SetLeave(e NodeCb) {
	v.leaveFunc = e
}

func (v *Visitor) ClearEnter() {
	v.enterFunc = emptyNodeCb
}

func (v *Visitor) ClearLeave() {
	v.leaveFunc = emptyNodeCb
}
