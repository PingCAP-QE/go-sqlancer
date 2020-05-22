package generator

import (
	"fmt"
	"strconv"
	"time"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser/mysql"
	tidb_types "github.com/pingcap/tidb/types"
)

type GenConfig struct {
	IsInExprIndex        bool
	EnableLeftRightJoin  bool
	IsInUpdateDeleteStmt bool
}

type GenCtx struct {
	GenConfig

	tmpTableIndex   int
	tmpColIndex     int
	UsedTables      []types.Table
	ResultTables    []types.Table
	TableAlias      map[string]string
	PivotRows       map[string]*connection.QueryItem
	unwrapPivotRows map[string]interface{}
}

func NewGenCtx(usedTables []types.Table, pivotRows map[string]*connection.QueryItem) *GenCtx {
	row := map[string]interface{}{}
	for key, value := range pivotRows {
		row[key], _ = getTypedValue(value)
	}
	return &GenCtx{
		tmpTableIndex:   0,
		tmpColIndex:     0,
		UsedTables:      usedTables,
		ResultTables:    make([]types.Table, 0),
		TableAlias:      make(map[string]string),
		PivotRows:       pivotRows,
		unwrapPivotRows: row,
		GenConfig: GenConfig{
			IsInExprIndex:        false,
			EnableLeftRightJoin:  true,
			IsInUpdateDeleteStmt: false,
		},
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

func getTypedValue(it *connection.QueryItem) (interface{}, byte) {
	if it.Null {
		return nil, mysql.TypeNull
	}
	switch it.ValType.DatabaseTypeName() {
	case "VARCHAR", "TEXT", "CHAR":
		return it.ValString, mysql.TypeString
	case "INT", "BIGINT", "TINYINT":
		i, _ := strconv.ParseInt(it.ValString, 10, 64)
		return i, mysql.TypeLong
	case "TIMESTAMP", "DATE", "DATETIME":
		t, _ := time.Parse("2006-01-02 15:04:05", it.ValString)
		return tidb_types.NewTime(tidb_types.FromGoTime(t), mysql.TypeTimestamp, 6), mysql.TypeDatetime
	case "FLOAT", "DOUBLE", "DECIMAL":
		f, _ := strconv.ParseFloat(it.ValString, 64)
		return f, mysql.TypeDouble
	default:
		panic(fmt.Sprintf("unreachable type %s", it.ValType.DatabaseTypeName()))
	}
}
