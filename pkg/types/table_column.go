package types

import "github.com/pingcap/parser/model"

type Table struct {
	Name    model.CIStr
	Columns [][3]string
	Indexes []model.CIStr
}

type TableColumn struct {
	Table string
	Name  string
}
