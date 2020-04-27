package types

import (
	"math/rand"
)

// Table defines database table
type Table struct {
	Name    CIStr
	Columns []Column
	Indexes []CIStr
	Type    string
}

// Clone copy table struct
func (t *Table) Clone() *Table {
	newTable := Table{
		Name:    t.Name,
		Columns: make([]Column, len(t.Columns)),
		Indexes: t.Indexes,
		Type:    t.Type,
	}
	for i, column := range t.Columns {
		newTable.Columns[i] = column.Clone()
	}
	return &newTable
}

// RandColumn rand column from table
func (t *Table) RandColumn() Column {
	if len(t.Columns) == 0 {
		panic("no columns in table")
	}
	return t.Columns[rand.Intn(len(t.Columns))]
}

// GetColumns get ordered columns
func (t *Table) GetColumns() []Column {
	return t.Columns
}
