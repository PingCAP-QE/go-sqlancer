package mutasql

import (
	"bytes"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
)

type Pool struct {
	Collection []TestCase
}

func (p *Pool) Length() int {
	return len(p.Collection)
}

type TestCase struct {
	D       []*Dataset
	Q       ast.Node
	Mutable bool // TODO: useful?
}

// TODO
func (t *TestCase) Print() string {
	return ""
}

// clone dataset also
func (t *TestCase) Clone() TestCase {
	newTestCase := TestCase{
		D:       make([]*Dataset, 0),
		Q:       cloneNode(t.Q),
		Mutable: t.Mutable,
	}
	for _, i := range t.D {
		d := i.Clone()
		newTestCase.D = append(newTestCase.D, &d)
	}
	return newTestCase
}

func (t *TestCase) ReplaceTableName(tableMap map[string]string) {
	t.Q = replaceTableNameInNode(t.Q, tableMap)
	for _, d := range t.D {
		d.ReplaceTableName(tableMap)
	}
}

func cloneNode(n ast.Node) ast.Node {
	out := new(bytes.Buffer)
	err := n.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		panic(zap.Error(err)) // should never get error
	}
	sql := out.String()
	p := parser.New()
	stmtNodes, _, _ := p.Parse(sql, "", "")

	return stmtNodes[0]
}

type Visitor struct {
	tableMap map[string]string
}

func (v *Visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch tp := in.(type) {
	case *ast.ColumnNameExpr:
		if name, ok := v.tableMap[tp.Name.Table.L]; ok {
			tp.Name.Table = model.NewCIStr(name)
		}
	case *ast.TableNameExpr:
		if name, ok := v.tableMap[tp.Name.Name.L]; ok {
			tp.Name.Name = model.NewCIStr(name)
		}
	case *ast.TableName:
		if name, ok := v.tableMap[tp.Name.L]; ok {
			tp.Name = model.NewCIStr(name)
		}
	case *ast.ColumnName:
		if name, ok := v.tableMap[tp.Table.L]; ok {
			tp.Table = model.NewCIStr(name)
		}
	}
	return in, false
}

func (v *Visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func replaceTableNameInNode(n ast.Node, tableMap map[string]string) ast.Node {
	v := Visitor{tableMap}
	n.Accept(&v)
	return n
}

func (r *TestCase) GetAllTables() []types.Table {
	tables := make([]types.Table, 0)
	tableExist := make(map[string]bool) // for remove duplicated
	for _, d := range r.D {
		if _, ok := tableExist[d.Table.Name.String()]; !ok {
			tables = append(tables, d.Table)
			tableExist[d.Table.Name.String()] = true
		}
	}
	return tables
}

type Dataset struct {
	Before []ast.Node // sql exec before insertion
	After  []ast.Node // sql exec after insertion
	Rows   map[string][]*connection.QueryItem
	Table  types.Table
}

func (d *Dataset) ReplaceTableName(tableMap map[string]string) {
	if tableName, ok := tableMap[d.Table.Name.String()]; ok {
		d.Table.Name = types.CIStr(tableName)
	}
	for _, node := range d.Before {
		node = replaceTableNameInNode(node, tableMap)
	}
	for _, node := range d.After {
		node = replaceTableNameInNode(node, tableMap)
	}
}

func (d *Dataset) Clone() Dataset {
	newDataset := Dataset{}
	for _, i := range d.Before {
		newDataset.Before = append(newDataset.Before, cloneNode(i))
	}
	for _, i := range d.After {
		newDataset.After = append(newDataset.After, cloneNode(i))
	}
	for k, i := range d.Rows {
		var items []*connection.QueryItem
		for _, j := range i {
			item := *j
			valType := *(j.ValType)
			item.ValType = &valType
			items = append(items, &item)
		}
		newDataset.Rows[k] = items
	}
	newDataset.Table = d.Table
	return newDataset
}

type Mutation interface {
	Condition(*TestCase) bool
	Mutate(*TestCase) (TestCase, error)
}
