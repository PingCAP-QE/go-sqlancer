package generator

import (
	"fmt"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
)

type GenCtx struct {
	tmpTableIndex int
	tmpColIndex   int
	UsedTables    []types.Table
	ResultTables  []types.Table
	TableAlias    map[string]string
	PivotRows     map[string]*connection.QueryItem

	IsInExprIndex       bool
	EnableLeftRightJoin bool
}

func NewGenCtx(enableLeftRightJoin bool, isInExprIndex bool, usedTables []types.Table, pivotRows map[string]*connection.QueryItem) *GenCtx {
	return &GenCtx{
		tmpTableIndex:       0,
		tmpColIndex:         0,
		UsedTables:          usedTables,
		ResultTables:        make([]types.Table, 0),
		TableAlias:          make(map[string]string),
		PivotRows:           pivotRows,
		IsInExprIndex:       isInExprIndex,
		EnableLeftRightJoin: enableLeftRightJoin,
	}
}

func (g *GenCtx) createTmpTable() string {
	g.tmpTableIndex++
	return fmt.Sprintf("tmp%d", g.tmpTableIndex)
}

func (g *GenCtx) createTmpColumn() string {
	g.tmpColIndex++
	return fmt.Sprintf("col_%d", g.tmpColIndex)
}

func (g *GenCtx) findUsedTableByName(name string) *types.Table {
	for _, table := range g.UsedTables {
		if table.Name.EqString(name) {
			t := table.Clone()
			return &t
		}
	}
	return nil
}
