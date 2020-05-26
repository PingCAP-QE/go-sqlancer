package generator

import (
	"sort"

	"github.com/pingcap/parser/ast"
)

func (g *Generator) CollectColumnNames(node ast.Node) []ast.ColumnName {
	collector := columnNameVisitor{
		Columns: make(map[string]ast.ColumnName),
	}
	node.Accept(&collector)
	var columns columnNames

	for _, column := range collector.Columns {
		columns = append(columns, column)
	}
	sort.Sort(columns)
	return columns
}

type columnNames []ast.ColumnName

func (c columnNames) Len() int {
	return len(c)
}

func (c columnNames) Less(i, j int) bool {
	return c[i].String() < c[j].String()
}

func (c columnNames) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type columnNameVisitor struct {
	Columns map[string]ast.ColumnName
}

func (v *columnNameVisitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *ast.ColumnName:
		if _, ok := v.Columns[n.String()]; !ok {
			v.Columns[n.String()] = *n
		}
	}
	return in, false
}

func (v *columnNameVisitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
