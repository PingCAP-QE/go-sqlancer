package types

import (
	"math/rand"

	"github.com/pingcap/parser/model"
)

// Table defines database table
type Table struct {
	Name    model.CIStr
	Columns [][3]string
	Indexes []model.CIStr
	Type    string
}

// Clone copy table struct
func (t *Table) Clone() *Table {
	newTable := Table{
		Name:    t.Name,
		Columns: make([][3]string, len(t.Columns)),
		Indexes: t.Indexes,
		Type:    t.Type,
	}
	for k := range t.Columns {
		cloneColumn := t.Columns[k]
		newTable.Columns[k] = cloneColumn
	}
	return &newTable
}

// RandColumn rand column from table
func (t *Table) RandColumn() [3]string {
	if len(t.Columns) == 0 {
		panic("no columns in table")
	}
	return t.Columns[rand.Intn(len(t.Columns))]
}

// GetColumns get ordered columns
func (t *Table) GetColumns() [][3]string {
	return t.Columns
}
