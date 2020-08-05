package sqlancer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/executor"
	"github.com/chaos-mesh/go-sqlancer/pkg/generator"
	"github.com/chaos-mesh/go-sqlancer/pkg/mutation"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/types/mutasql"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	parserTypes "github.com/pingcap/parser/types"
	"go.uber.org/zap"
)

type MutaSql struct {
	generator.Generator
	conf     *Config
	executor *executor.Executor

	pool      *mutasql.Pool
	mutations []mutasql.Mutation
}

func NewMutaSql(conf *Config) (*MutaSql, error) {
	log.InitLogger(&log.Config{Level: conf.LogLevel, File: log.FileLogConfig{}})
	e, err := executor.New(conf.DSN, conf.DBName)
	if err != nil {
		return nil, err
	}
	return &MutaSql{
		conf:      conf,
		executor:  e,
		Generator: generator.Generator{Config: generator.Config{Hint: conf.EnableHint}},
		pool:      new(mutasql.Pool),
		mutations: []mutasql.Mutation{
			&mutation.Rollback{},
			&mutation.AdditionSelect{},
		},
	}, nil
}

func (m *MutaSql) Start(ctx context.Context) {
	m.init()
	m.run(ctx)
	m.tearDown()
}

func (m *MutaSql) init() {
	m.makeSeedQuery()
}

func (m *MutaSql) makeSeedQuery() {
	// TODO
}

func (m *MutaSql) run(ctx context.Context) {
	m.refreshDB(ctx)
	for {
		i := 0
		select {
		case <-ctx.Done():
			return
		default:
			if i >= 50 {
				i = 0
				m.refreshDB(ctx)
			}
			m.progress()
			i++
		}
	}
}

func (m *MutaSql) tearDown() {
	m.executor.Close()
}

func (m *MutaSql) refreshDB(ctx context.Context) {
	log.L().Debug("refresh database")
	m.setUpDB(ctx)
}

func (m *MutaSql) setUpDB(ctx context.Context) {
	_ = m.executor.Exec("drop database if exists " + m.conf.DBName)
	_ = m.executor.Exec("create database " + m.conf.DBName)
	_ = m.executor.Exec("use " + m.conf.DBName)
}

func (m *MutaSql) verify(a, b []connection.QueryItems) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}

		for j := range a[i] {
			if a[i][j].MustSame(b[i][j]) != nil {
				return false
			}
		}
	}
	return true
}

func (m *MutaSql) progress() {
	if m.pool.Length() == 0 {
		panic("no testcases in pool")
	}
	retry := 0
	for {
		testCase := m.pool.Collection[util.Rd(m.pool.Length())]
		validMutations := make([]mutasql.Mutation, 0)
		for _, i := range m.mutations {
			if i.Condition(&testCase) {
				validMutations = append(validMutations, i)
			}
		}
		if len(validMutations) == 0 {
			log.L().Warn("no mutation satisfied")
			retry++
			if retry > 5 {
				panic("failed to choose mutation for 5 times")
			}
			continue
		}
		retry = 0
		mutation := validMutations[util.Rd(len(validMutations))]
		// TODO: remove duplicated testcases
		newTestCase, err := mutation.Mutate(&testCase, &m.Generator)
		if err != nil {
			log.L().Error("mutate error", zap.Error(err))
			continue
		}

		originCase := testCase.Clone()

		originResult, err := m.applyTestCase(&originCase)
		if err != nil {
			log.L().Error("execute error", zap.Error(err))
			m.PrintError(&originCase, &newTestCase)
			panic("exec error")
		}
		newResult, err := m.applyTestCase(&newTestCase)
		if err != nil {
			log.L().Error("execute error", zap.Error(err))
			m.PrintError(&originCase, &newTestCase)
			panic("exec error")
		}

		if !m.verify(originResult, newResult) {
			log.L().Error("verify failed")
			m.PrintError(&originCase, &newTestCase, originResult, newResult)
			panic("verify error")
		}
	}
}

func (m *MutaSql) randTableNames(names []types.CIStr) map[string]string {
	result := make(map[string]string)
	for _, name := range names {
		if _, ok := result[name.String()]; !ok {
			result[name.String()] = util.RdStringChar(8)
		}
	}
	return result
}

func (m *MutaSql) applyTestCase(t *mutasql.TestCase) ([]connection.QueryItems, error) {
	tables := t.GetAllTables()
	originTableNames := make([]types.CIStr, 0)
	for _, i := range tables {
		originTableNames = append(originTableNames, i.Name)
	}
	tableNames := m.randTableNames(originTableNames)

	renamedCase := t.Clone()
	renamedCase.ReplaceTableName(tableNames)

	if err := m.execSQLsWithErrLog(renamedCase.BeforeInsert); err != nil {
		return nil, err
	}

	err := m.populateData(&renamedCase)
	if err != nil {
		return nil, err
	}

	if err = m.execSQLsWithErrLog(renamedCase.AfterInsert); err != nil {
		return nil, err
	}

	sql, err := util.BufferOut(renamedCase.Q)
	if err != nil {
		return nil, err
	}

	log.L().Debug("SQL EXEC: " + sql)
	res, err := m.executor.GetConn().Select(sql)
	if err != nil {
		return nil, err
	}

	if err = m.execSQLsWithErrLog(renamedCase.CleanUp); err != nil {
		return nil, err
	}

	return res, nil
}

func (m *MutaSql) execSQLsWithErrLog(nodes []ast.Node) error {
	for _, node := range nodes {
		sql, err := util.BufferOut(node)
		if err != nil {
			return err
		}

		log.L().Debug("SQL EXEC: " + sql)
		err = m.executor.Exec(sql)
		if err != nil {
			log.L().Error("sql execute error", zap.Error(err))
			return err
		}
	}
	return nil
}

// create table
func (m *MutaSql) populateData(tc *mutasql.TestCase) error {
	var err error
	// exec Before sqls
	for _, dataset := range tc.D {
		if err = m.execSQLsWithErrLog(dataset.Before); err != nil {
			return err
		}

		err = m.createSchema(dataset.Table)
		if err != nil {
			log.L().Error("create schema error", zap.Error(err))
			return err
		}
		err = m.insertData(dataset.Table, dataset.Rows)
		if err != nil {
			log.L().Error("insert data error", zap.Error(err))
			return err
		}

		if err = m.execSQLsWithErrLog(dataset.After); err != nil {
			return err
		}
	}
	return nil
}

// TODO: if mutation would change CREATE TABLE clause? (such as expression index)
// use `ALTER TABLE xxx ADD PRIMARY KEY(xxx)` to add primary key in Dataset.After
// use `CREATE INDEX xxx ON xxx`
func (m *MutaSql) createSchema(t types.Table) error {
	node := ast.CreateTableStmt{
		Table:       &ast.TableName{Name: model.NewCIStr(t.Name.String())},
		Cols:        []*ast.ColumnDef{},
		Constraints: []*ast.Constraint{},
		Options:     []*ast.TableOption{},
	}

	for _, col := range t.Columns {
		fieldType := parserTypes.NewFieldType(executor.Type2Tp(col.Type))
		fieldType.Flen = executor.DataType2Len(col.Type)
		node.Cols = append(node.Cols, &ast.ColumnDef{
			Name: &ast.ColumnName{Name: model.NewCIStr(col.Name.String())},
			Tp:   fieldType,
		})
	}

	sql, err := util.BufferOut(&node)
	if err != nil {
		return err
	}

	log.L().Debug("SQL EXEC: " + sql)
	return m.executor.Exec(sql)
}

func (m *MutaSql) insertData(t types.Table, columns map[string][]*connection.QueryItem) error {
	node := ast.InsertStmt{
		Table: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableName{Name: model.NewCIStr(t.Name.String())},
			},
		},
		Lists:   [][]ast.ExprNode{},
		Columns: []*ast.ColumnName{},
	}
	columnNames := make([]string, 0)
	for k := range columns {
		columnNames = append(columnNames, k)
		node.Columns = append(node.Columns, &ast.ColumnName{
			Table: model.NewCIStr(t.Name.String()),
			Name:  model.NewCIStr(k),
		})
	}

	if len(columnNames) == 0 {
		return nil
	}

	dataLen := len(columns[columnNames[0]])
	for i := 0; i < dataLen; i++ {
		// assume number of columnNames equals real column number in Table.Columns
		var values []ast.ExprNode
		for _, col := range columnNames {
			if err, val := transQueryItemToValue(columns[col][i]); err == nil {
				values = append(values, ast.NewValueExpr(val, "", ""))
			} else {
				return err
			}
		}
		node.Lists = append(node.Lists, values)
	}

	sql, err := util.BufferOut(&node)
	if err != nil {
		return err
	}

	log.L().Debug("SQL EXEC: " + sql)
	return m.executor.Exec(sql)
}

func (m *MutaSql) PrintError(origin, mutated *mutasql.TestCase, results ...[]connection.QueryItems) {
	log.L().Error("origin: " + origin.String())
	log.L().Error("mutated: " + mutated.String())
	for i, res := range results {
		output := fmt.Sprintf("Query Result_%d:", i)
		for j, items := range res {
			output += fmt.Sprintf("\n  [%d]:\n  ", j)
			for _, item := range items {
				output += fmt.Sprintf("  (%s)", item.String())
			}
		}
		log.L().Error(output)
	}
}

func transQueryItemToValue(q *connection.QueryItem) (error, interface{}) {
	if q.Null {
		return nil, nil
	}

	switch strings.ToUpper(q.ValType.DatabaseTypeName()) {
	case "VARCHAR", "CHAR", "TEXT":
		return nil, q.ValString
	case "SMALLINT", "INT", "BIGINT":
		val, err := strconv.ParseInt(q.ValString, 10, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid int format: %s", q.ValString)), nil
		}
		return nil, val
	case "BOOL":
		if q.ValString == "0" || strings.ToUpper(q.ValString) == "FALSE" {
			return nil, false
		}
		if q.ValString == "1" || strings.ToUpper(q.ValString) == "TRUE" {
			return nil, true
		}
		return errors.New(fmt.Sprintf("unrecognized bool value:%s", q.ValString)), nil
	case "FLOAT", "DECIMAL":
		val, err := strconv.ParseFloat(q.ValString, 64)
		if err != nil {
			return errors.New(fmt.Sprintf("invalid float format: %s", q.ValString)), nil
		}
		return nil, val
	}
	return errors.New(fmt.Sprintf("unknown type: %s", q.ValType.DatabaseTypeName())), nil
}
