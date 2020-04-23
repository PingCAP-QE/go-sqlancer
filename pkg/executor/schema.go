package executor

import (
	"regexp"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
)

var (
	typePattern = regexp.MustCompile(`\(\d+\)`)
)

// ReloadSchema expose reloadSchema
func (e *Executor) ReloadSchema() error {
	return errors.Trace(e.reloadSchema())
}

func (e *Executor) reloadSchema() error {
	schema, err := e.conn.FetchSchema(e.db)
	if err != nil {
		return errors.Trace(err)
	}
	indexes := make(map[string][]model.CIStr)
	for _, col := range schema {
		if _, ok := indexes[col[2]]; ok {
			continue
		}
		index, err := e.conn.FetchIndexes(e.db, col[1])
		// may not return error here
		// just disable indexes
		if err != nil {
			return errors.Trace(err)
		}
		var modelIndex []model.CIStr
		for _, indexName := range index {
			modelIndex = append(modelIndex, model.NewCIStr(indexName))
		}
		indexes[col[1]] = modelIndex
	}

	e.loadSchema(schema, indexes)
	return nil
}

func (e *Executor) loadSchema(records [][6]string, indexes map[string][]model.CIStr) {
	// init databases
LOOP:
	for _, record := range records {
		dbname := record[0]
		if dbname != e.db {
			continue
		}
		tableName := record[1]
		tableType := record[2]
		columnName := record[3]
		columnType := record[4]
		columnNull := record[5]
		options := make([]ast.ColumnOptionType, 0)
		if record[5] == "NO" {
			options = append(options, ast.ColumnOptionNotNull)
		}
		index, ok := indexes[tableName]
		if !ok {
			index = []model.CIStr{}
		}
		if _, ok := e.tables[tableName]; !ok {
			e.tables[tableName] = &types.Table{
				Name:    model.NewCIStr(tableName),
				Columns: [][3]string{},
				Indexes: index,
				Type:    tableType,
			}
		}

		for index, column := range e.tables[tableName].Columns {
			if column[0] == columnName {
				e.tables[tableName].Columns[index][1] = columnType
				e.tables[tableName].Columns[index][2] = columnNull
				continue LOOP
			}
		}
		e.tables[tableName].Columns = append(e.tables[tableName].Columns,
			[3]string{columnName, columnType, columnNull})
	}
}
