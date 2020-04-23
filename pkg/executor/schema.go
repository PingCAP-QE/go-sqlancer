package executor

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"

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
	indexes := make(map[string][]string)
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
		indexes[col[1]] = index
	}

	e.loadSchema(schema, indexes)
	return nil
}

func (e *Executor) loadSchema(records [][6]string, indexes map[string][]string) {
	// init databases
	for _, record := range records {
		dbname := record[0]
		if dbname != e.db {
			continue
		}
		tableName := record[1]
		tableType := record[2]
		columnName := record[3]
		columnType := record[4]
		options := make([]ast.ColumnOptionType, 0)
		if record[5] == "NO" {
			options = append(options, ast.ColumnOptionNotNull)
		}
		index, ok := indexes[tableName]
		if !ok {
			index = []string{}
		}
		if _, ok := e.tables[tableName]; !ok {
			e.tables[tableName] = &types.Table{
				DB:      dbname,
				Table:   tableName,
				Type:    tableType,
				Columns: make(map[string]*types.Column),
				Indexes: index,
			}
		}
		if _, ok := e.tables[tableName].Columns[columnName]; !ok {
			dataLengthStr := typePattern.FindString(columnType)
			length := 0
			if dataLengthStr != "" {
				length, _ = strconv.Atoi(dataLengthStr[1 : len(dataLengthStr)-1])
			}
			e.tables[tableName].Columns[columnName] = &types.Column{
				DB:     dbname,
				Table:  tableName,
				Column: columnName,
				// remove the data size in type definition
				DataType: typePattern.ReplaceAllString(strings.ToLower(columnType), ""),
				DataLen:  length,
				Options:  options,
			}
		}
	}
}
