package types

import (
	"math/rand"
)

// Table defines database table
type Table struct {
	Name      CIStr
	AliasName CIStr
	Columns   []Column
	Indexes   []CIStr
	Type      string
}

// Clone copy table struct
func (t Table) Clone() Table {
	newTable := Table{
		Name:      t.Name,
		AliasName: t.AliasName,
		Columns:   make([]Column, len(t.Columns)),
		Indexes:   t.Indexes,
		Type:      t.Type,
	}
	for i, column := range t.Columns {
		newTable.Columns[i] = column.Clone()
	}
	return newTable
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

func (t Table) Rename(name string) Table {
	newTable := Table{
		Name:      t.Name,
		AliasName: CIStr(name),
		Columns:   make([]Column, len(t.Columns)),
		Indexes:   t.Indexes,
		Type:      t.Type,
	}
	for i, column := range t.Columns {
		col := column.Clone()
		col.AliasTable = CIStr(name)
		newTable.Columns[i] = col
	}
	return newTable
}

func (a Table) JoinWithName(b Table, name string) Table {
	var cols []Column
	for _, col := range a.Columns {
		c := col.Clone()
		c.AliasTable = CIStr(name)
		cols = append(cols, c)
	}
	for _, col := range b.Columns {
		c := col.Clone()
		c.AliasTable = CIStr(name)
		cols = append(cols, c)
	}
	return Table{
		Name:    CIStr(name),
		Columns: cols,
		Indexes: nil,
		Type:    "TMP TABLE",
	}
}

// GetAliasName get tmp table name, otherwise origin name
func (t Table) GetAliasName() CIStr {
	if t.AliasName != "" {
		return t.AliasName
	}
	return t.Name
}
