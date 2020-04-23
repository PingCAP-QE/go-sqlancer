package pivot

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"

	"github.com/chaos-mesh/private-wreck-it/pkg/connection"
	"github.com/chaos-mesh/private-wreck-it/pkg/executor"
)

type Pivot struct {
	wg       sync.WaitGroup
	Conf     *Config
	DB       *sql.DB
	Executor *executor.Executor
	round    int

	Generator
}

// NewPivot ...
func NewPivot(conf *Config) (*Pivot, error) {
	e, err := executor.New(conf.DSN, conf.DBName)
	if err != nil {
		return nil, err
	}
	return &Pivot{
		Conf:      conf,
		Executor:  e,
		Generator: Generator{},
	}, nil
}

const (
	tableSQL        = "DESC %s.%s"
	indexSQL        = "SHOW INDEX FROM %s.%s"
	schemaSQL       = "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM information_schema.tables"
	indexColumnName = "Key_name"
)

// Start Pivot
func (p *Pivot) Start(ctx context.Context) {
	p.cleanup(ctx)
	p.kickup(ctx)
}

// Close Pivot
func (p *Pivot) Close() {
	p.wg.Wait()
	p.cleanup(context.Background())
	p.Executor.Close()

}

// Init Pivot
func (p *Pivot) Init(ctx context.Context) {
	rand.Seed(time.Now().UnixNano())
	p.Tables = make([]Table, 0)

	// Warn: Hard code db name
	tables, err := p.Executor.GetConn().FetchTables(p.Conf.DBName)
	if err != nil {
		panic(err)
	}
	for _, i := range tables {
		t := Table{Name: model.NewCIStr(i)}
		t.Columns, err = p.Executor.GetConn().FetchColumns(p.Conf.DBName, i)
		if err != nil {
			panic(err)
		}
		idx, err := p.Executor.GetConn().FetchIndexes(p.Conf.DBName, i)
		if err != nil {
			panic(err)
		}
		for _, j := range idx {
			t.Indexes = append(t.Indexes, model.NewCIStr(j))
		}
		p.Tables = append(p.Tables, t)
	}
}

func (p *Pivot) prepare(ctx context.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	g, _ := errgroup.WithContext(ctx)
	for index, columnTypes := range ComposeAllColumnTypes(-1) {
		tableIndex := index
		colTs := make([]string, len(columnTypes))
		copy(colTs, columnTypes)
		g.Go(func() error {
			sql, _ := p.Executor.GenerateDDLCreateTable(tableIndex, colTs)
			return p.Executor.Exec(sql.SQLStmt)
		})
	}
	if err := g.Wait(); err != nil {
		log.L().Error("create table failed", zap.Error(err))
	}

	err := p.Executor.ReloadSchema()
	if err != nil {
		log.Error("reload data failed!")
	}
	for i := 0; i < r.Intn(10); i++ {
		sql, _ := p.Executor.GenerateDDLCreateIndex()
		fmt.Println(sql)
		err = p.Executor.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("create index failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
		}
	}

	for _, table := range p.Executor.GetTables() {
		sql, err := p.Executor.GenerateDMLInsertByTable(table.Table)
		if err != nil {
			panic(errors.ErrorStack(err))
		}
		err = p.Executor.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("insert data failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
		}
	}
}

func (p *Pivot) cleanup(ctx context.Context) {
	p.Executor.Exec("drop database if exists " + p.Conf.DBName)
	p.Executor.Exec("create database " + p.Conf.DBName)
	p.Executor.Exec("use " + p.Conf.DBName)
}

func (p *Pivot) kickup(ctx context.Context) {
	p.wg.Add(1)
	p.prepare(ctx)
	p.Init(ctx)

	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				for {
					p.round++
					p.progress(ctx)
				}
			}
		}

	}()
}

func (p *Pivot) progress(ctx context.Context) {
	// rand one pivot row for one table
	pivotRows, usedTables, err := p.ChoosePivotedRow()
	if err != nil {
		panic(err)
	}
	// generate sql ast tree and
	// generate sql where clause
	selStmt, selectSQL, columns, err := p.GenSelectStmt(pivotRows, usedTables)
	if err != nil {
		panic(err)
	}
	// execute sql, ensure not null result set
	resultRows, err := p.execSelect(selectSQL)
	if err != nil {
		log.L().Error("execSelect failed", zap.Error(err))
		return
	}
	// verify pivot row in result row set
	correct := p.verify(pivotRows, columns, resultRows)
	fmt.Printf("Round %d, verify result %v!\n", p.round, correct)
	if !correct {
		subSQL, err := p.simplifySelect(selStmt, pivotRows, usedTables, columns)
		if err != nil {
			log.Error("occurred an error when try to simplify select", zap.String("sql", selectSQL), zap.Error(err))
			fmt.Printf("query:\n%s\n", selectSQL)
		} else {
			fmt.Printf("query:\n%s\n", selectSQL)
			if len(subSQL) < len(selectSQL) {
				fmt.Printf("sub query:\n%s\n", subSQL)
			}
		}
		fmt.Printf("row:\n")
		for column, value := range pivotRows {
			fmt.Printf("%s.%s:%v\n", column.Table, column.Name, value.ValString)
		}
		return
		//panic("data verified failed")
	}
	if p.round <= p.Conf.ViewCount {
		if err := p.Executor.GetConn().CreateViewBySelect(fmt.Sprintf("view_%d", p.round), selectSQL, len(resultRows)); err != nil {
			fmt.Println("create view failed")
			panic(err)
		}
	}
	if p.round == p.Conf.ViewCount {
		if err := p.Executor.ReloadSchema(); err != nil {
			panic(err)
		}
	}
	// log.Info("run one statement successfully!", zap.String("query", selectStmt))
}

// ChoosePivotedRow choose a row
// it may move to another struct
func (p *Pivot) ChoosePivotedRow() (map[TableColumn]*connection.QueryItem, []Table, error) {
	result := make(map[TableColumn]*connection.QueryItem)
	count := 1
	if len(p.Tables) > 1 {
		// avoid too deep joins
		if count = Rd(len(p.Tables)-1) + 1; count > 4 {
			count = Rd(4) + 1
		}
	}
	rand.Shuffle(len(p.Tables), func(i, j int) { p.Tables[i], p.Tables[j] = p.Tables[j], p.Tables[i] })
	usedTables := p.Tables[:count]
	var reallyUsed []Table

	for _, i := range usedTables {
		sql := fmt.Sprintf("SELECT * FROM %s ORDER BY RAND() LIMIT 1;", i.Name)
		exeRes, err := p.execSelect(sql)
		if err != nil {
			panic(err)
		}
		if len(exeRes) > 0 {
			for _, c := range exeRes[0] {
				// panic(fmt.Sprintf("no rows in table %s", i.Name))
				tableColumn := TableColumn{i.Name.O, c.ValType.Name()}
				result[tableColumn] = c
			}
			reallyUsed = append(reallyUsed, i)

		}
	}
	return result, reallyUsed, nil
}

func (p *Pivot) GenSelectStmt(pivotRows map[TableColumn]*connection.QueryItem, usedTables []Table) (*ast.SelectStmt, string, []TableColumn, error) {
	stmtAst, err := p.selectStmtAst(p.Conf.Depth, usedTables)
	if err != nil {
		return nil, "", nil, err
	}
	sql, columns, err := p.selectStmt(&stmtAst, usedTables, pivotRows)
	if err != nil {
		return nil, "", nil, err
	}
	return &stmtAst, sql, columns, nil
}

func (p *Pivot) ExecAndVerify(stmt *ast.SelectStmt, originRow map[TableColumn]*connection.QueryItem, columns []TableColumn) (bool, error) {
	sql, err := BufferOut(stmt)
	if err != nil {
		return false, err
	}
	resultSets, err := p.execSelect(sql)
	if err != nil {
		return false, err
	}
	res := p.verify(originRow, columns, resultSets)
	return res, nil
}

// may not return string
func (p *Pivot) execSelect(stmt string) ([][]*connection.QueryItem, error) {
	return p.Executor.GetConn().Select(stmt)
}

func (p *Pivot) verify(originRow map[TableColumn]*connection.QueryItem, columns []TableColumn, resultSets [][]*connection.QueryItem) bool {
	for _, row := range resultSets {
		if p.checkRow(originRow, columns, row) {
			return true
		}
	}
	//fmt.Println("=========  ORIGIN ROWS ======")
	//for k, v := range originRow {
	//	fmt.Printf("key: %+v, value: [null: %v, value: %s]\n", k, v.Null, v.ValString)
	//}
	//
	//fmt.Println("=========  COLUMNS ======")
	//for _, c := range columns {
	//	fmt.Printf("Table: %s, Name: %s\n", c.Table, c.Name)
	//}
	// fmt.Printf("=========  DATA ======, count: %d\n", len(resultSets))
	// for i, r := range resultSets {
	// 	fmt.Printf("$$$$$$$$$ line %d\n", i)
	// 	for j, c := range r {
	// 		fmt.Printf("  table: %s, field: %s, field: %s, value: %s\n", columns[j].Table, columns[j].Name, c.ValType.Name(), c.ValString)
	// 	}
	// }

	return false
}

func (p *Pivot) checkRow(originRow map[TableColumn]*connection.QueryItem, columns []TableColumn, resultSet []*connection.QueryItem) bool {
	for i, c := range columns {
		// fmt.Printf("i: %d, column: %+v, left: %+v, right: %+v", i, c, originRow[c], resultSet[i])
		if !compareQueryItem(originRow[c], resultSet[i]) {
			return false
		}
	}
	return true
}

func (p *Pivot) simplifySelect(stmt *ast.SelectStmt, pivotRows map[TableColumn]*connection.QueryItem, usedTable []Table, columns []TableColumn) (string, error) {
	selectStmt := p.trySubSelect(stmt, stmt.Where, usedTable, pivotRows, columns)
	// for those ast that we don't try to simplify
	if selectStmt == nil {
		selectStmt = stmt
	}
	p.RectifyCondition(selectStmt, usedTable, pivotRows)
	sql, err := BufferOut(selectStmt)
	if err != nil {
		return "", err
	}
	return sql, nil
}

func (p *Pivot) trySubSelect(stmt *ast.SelectStmt, e ast.Node, usedTable []Table, pivotRows map[TableColumn]*connection.QueryItem, columns []TableColumn) *ast.SelectStmt {
	switch t := e.(type) {
	case *ast.ParenthesesExpr:
		result := p.trySubSelect(stmt, t.Expr, usedTable, pivotRows, columns)
		return result
	case *ast.BinaryOperationExpr:
		subSelectStmt := p.trySubSelect(stmt, t.L, usedTable, pivotRows, columns)
		if subSelectStmt != nil {
			return subSelectStmt
		}
		subSelectStmt = p.trySubSelect(stmt, t.R, usedTable, pivotRows, columns)
		if subSelectStmt != nil {
			return subSelectStmt
		}
		subSelectStmt = &ast.SelectStmt{
			SelectStmtOpts: &ast.SelectStmtOpts{
				SQLCache: true,
			},
			From:   stmt.From,
			Fields: stmt.Fields,
			Where:  t,
		}
		exist, err := p.ExecAndVerify(subSelectStmt, pivotRows, columns)
		if err != nil {
			log.L().Error("occurred an error", zap.Error(err))
			return nil
		}
		if !p.verifyExistence(subSelectStmt, usedTable, pivotRows, exist) {
			return subSelectStmt
		}
		return nil
	case *ast.UnaryOperationExpr:
		subSelectStmt := p.trySubSelect(stmt, t.V, usedTable, pivotRows, columns)
		if subSelectStmt != nil {
			return subSelectStmt
		}
		subSelectStmt = &ast.SelectStmt{
			SelectStmtOpts: &ast.SelectStmtOpts{
				SQLCache: true,
			},
			From:   stmt.From,
			Fields: stmt.Fields,
			Where:  t,
		}
		exist, err := p.ExecAndVerify(subSelectStmt, pivotRows, columns)
		if err != nil {
			log.L().Error("occurred an error", zap.Error(err))
			return nil
		}
		if !p.verifyExistence(subSelectStmt, usedTable, pivotRows, exist) {
			return subSelectStmt
		}
		return nil
	default:
		return nil
	}
}

func (p *Pivot) verifyExistence(sel *ast.SelectStmt, usedTables []Table, pivotRows map[TableColumn]*connection.QueryItem, exist bool) bool {
	where := sel.Where
	out := Evaluate(where, usedTables, pivotRows)

	switch out.Kind() {
	case tidb_types.KindNull:
		return exist == false
	default:
		// make it true
		zero := parser_driver.ValueExpr{}
		zero.SetInt64(0)
		res, _ := out.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &zero.Datum)
		if res == 0 {
			return exist == false
		}
		return exist
	}
}

func compareQueryItem(left *connection.QueryItem, right *connection.QueryItem) bool {
	if left.ValType.Name() != right.ValType.Name() {
		return false
	}
	if left.Null != right.Null {
		return false
	}

	return (left.Null && right.Null) || (left.ValString == right.ValString)
}
