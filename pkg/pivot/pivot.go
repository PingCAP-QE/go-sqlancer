package pivot

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/executor"
	"github.com/chaos-mesh/go-sqlancer/pkg/generator"
	"github.com/chaos-mesh/go-sqlancer/pkg/knownbugs"
	. "github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
)

const NOREC_TMP_TABLE_NAME = "tmp_table"
const NOREC_TMP_COL_NAME = "tmp_col"

var (
	allColumnTypes = []string{"int", "float", "varchar"}
)

type Pivot struct {
	wg       sync.WaitGroup
	Conf     *Config
	DB       *sql.DB
	Executor *executor.Executor
	round    int
	batch    int
	inWrite  sync.RWMutex

	generator.Generator
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
		Generator: generator.Generator{Config: generator.Config{Hint: conf.Hint}},
	}, nil
}

// Start Pivot
func (p *Pivot) Start(ctx context.Context) {
	p.cleanup(ctx)
	p.kickup(ctx)
}

// Close Pivot
func (p *Pivot) Close() {
	p.wg.Wait()
	p.Executor.Close()
}

// LoadSchema load table/view/index schema
func (p *Pivot) LoadSchema(ctx context.Context) {
	rand.Seed(time.Now().UnixNano())
	p.Tables = make([]Table, 0)

	tables, err := p.Executor.GetConn().FetchTables(p.Conf.DBName)
	if err != nil {
		panic(err)
	}
	for _, i := range tables {
		t := Table{Name: CIStr(i)}
		columns, err := p.Executor.GetConn().FetchColumns(p.Conf.DBName, i)
		if err != nil {
			panic(err)
		}
		for _, column := range columns {
			col := Column{
				Table: CIStr(i),
				Name:  CIStr(column[0]),
				Null:  strings.EqualFold(column[2], "Yes"),
			}
			col.ParseType(column[1])
			t.Columns = append(t.Columns, col)
		}
		idx, err := p.Executor.GetConn().FetchIndexes(p.Conf.DBName, i)
		if err != nil {
			panic(err)
		}
		for _, j := range idx {
			t.Indexes = append(t.Indexes, CIStr(j))
		}
		p.Tables = append(p.Tables, t)
	}
}

func (p *Pivot) prepare(ctx context.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	g, _ := errgroup.WithContext(ctx)
	for index, columnTypes := range ComposeAllColumnTypes(-1, allColumnTypes) {
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
		sql, err := p.Executor.GenerateDDLCreateIndex()
		if err != nil {
			fmt.Println(err)
		}
		err = p.Executor.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("create index failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
		}
	}

	p.LoadSchema(ctx)

	if err := p.Executor.GetConn().Begin(); err != nil {
		log.L().Error("begin txn failed", zap.Error(err))
		return
	}
	for _, table := range p.Executor.GetTables() {
		insertData := func() {
			sql, err := p.Executor.GenerateDMLInsertByTable(table.Name.String())
			if err != nil {
				panic(errors.ErrorStack(err))
			}
			err = p.Executor.Exec(sql.SQLStmt)
			if err != nil {
				log.L().Error("insert data failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
			}
		}
		insertData()

		// update or delete
		for i := Rd(4); i > 0; i-- {
			tables := p.randTables()

			if err != nil {
				panic(errors.Trace(err))
			}
			if len(tables) == 0 {
				log.L().Panic("tables random by ChoosePivotedRow is empty")
			}
			var dmlStmt string
			switch Rd(2) {
			case 0:
				dmlStmt, err = p.DeleteDMLStmt(tables, *table)
				if err != nil {
					// TODO: goto next generation
					log.L().Error("generate delete stmt failed", zap.Error(err))
				}
			default:
				dmlStmt, err = p.UpdateDMLStmt(tables, *table)
				if err != nil {
					// TODO: goto next generation
					log.L().Error("generate update stmt failed", zap.Error(err))
				}
			}
			log.L().Info("Update/Delete statement", zap.String(table.Name.String(), dmlStmt))
			err = p.Executor.Exec(dmlStmt)
			if err != nil {
				log.L().Error("update/delete data failed", zap.String("sql", dmlStmt), zap.Error(err))
				panic(err)
			}
		}

		countSQL := "select count(*) from " + table.Name.String()
		qi, err := p.Executor.GetConn().Select(countSQL)
		if err != nil {
			log.L().Error("insert data failed", zap.String("sql", countSQL), zap.Error(err))
		}
		count := qi[0][0].ValString
		if p.Conf.Debug {
			log.L().Debug("table check records count", zap.String(table.Name.String(), count))
		}

		if c, _ := strconv.ParseUint(count, 10, 64); c == 0 {
			log.L().Info(table.Name.String() + " is empty after DELETE")
			insertData()
		}
	}
	if err := p.Executor.GetConn().Commit(); err != nil {
		log.L().Error("commit txn failed", zap.Error(err))
		return
	}
}

func (p *Pivot) cleanup(ctx context.Context) {
	_ = p.Executor.Exec("drop database if exists " + p.Conf.DBName)
	_ = p.Executor.Exec("create database " + p.Conf.DBName)
	_ = p.Executor.Exec("use " + p.Conf.DBName)
}

func (p *Pivot) kickup(ctx context.Context) {
	p.wg.Add(1)
	p.refreshDatabase(ctx)

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
					if p.round > 100 {
						p.refreshDatabase(ctx)
						p.round = 0
						p.batch++
					}
				}
			}
		}

	}()
}

func (p *Pivot) progress(ctx context.Context) {
	p.inWrite.RLock()
	defer func() {
		p.inWrite.RUnlock()
	}()

	// rand one pivot row for one table
	pivotRows, usedTables, err := p.ChoosePivotedRow()
	if err != nil {
		panic(err)
	}
	var selectAst *ast.SelectStmt
	var selectSQL string
	var columns []Column
	var updatedPivotRows map[string]*connection.QueryItem
	var tableMap *generator.GenCtx
	isNoREC := false
	if p.Conf.NoREC && RdBool() && p.round > p.Conf.ViewCount {
		isNoREC = true
		selectAst, selectSQL, err = p.GenNoRecNormalSelectStmt(usedTables)
	} else {
		isNoREC = false
		// generate sql ast tree and
		// generate sql where clause
		selectAst, selectSQL, columns, updatedPivotRows, tableMap, err = p.GenSelectStmt(pivotRows, usedTables)
	}
	if err != nil {
		panic(err)
	}
	dust := knownbugs.NewDustbin([]ast.Node{selectAst}, pivotRows)
	// execute sql, ensure not null result set
	if explicitTxn := RdBool(); explicitTxn {
		if err = p.Executor.GetConn().Begin(); err != nil {
			log.L().Error("begin txn failed", zap.Error(err))
			return
		}
		if p.Conf.Debug {
			log.L().Debug("begin txn success")
		}
		defer func() {
			if err = p.Executor.GetConn().Commit(); err != nil {
				log.L().Error("commit txn failed", zap.Error(err))
				return
			}
			if p.Conf.Debug {
				log.L().Debug("commit txn success")
			}
		}()
	}
	resultRows, err := p.execSelect(selectSQL)
	if err != nil {
		log.L().Error("execSelect failed", zap.Error(err))
		return
	}
	correct := false
	if isNoREC {
		correct = p.verifyNoREC(selectAst, resultRows, usedTables)
	} else {
		// verify pivot row in result row set
		correct = p.verify(updatedPivotRows, columns, resultRows)
	}
	fmt.Printf("Batch %d, Round %d, isNoRec: %v, verify result %v!\n", p.batch, p.round, isNoREC, correct)
	if !correct {
		// subSQL, err := p.minifySelect(selStmt, pivotRows, usedTables, columns)
		// if err != nil {
		// 	log.Error("occurred an error when try to simplify select", zap.String("sql", selectSQL), zap.Error(err))
		// 	fmt.Printf("query:\n%s\n", selectSQL)
		// } else {
		// 	fmt.Printf("query:\n%s\n", selectSQL)
		// 	if len(subSQL) < len(selectSQL) {
		// 		fmt.Printf("sub query:\n%s\n", subSQL)
		// 	}
		// }
		if !isNoREC && dust.IsKnownBug() {
			return
		}
		fmt.Printf("row:\n")
		p.printPivotRows(pivotRows)
		if p.Conf.Silent && p.round >= p.Conf.ViewCount {
			return
		}
		panic("data verified failed")
	}
	if p.round <= p.Conf.ViewCount {
		if err := p.Executor.GetConn().CreateViewBySelect(fmt.Sprintf("view_%d", p.round), selectSQL, len(resultRows), columns); err != nil {
			fmt.Println("create view failed", tableMap)
			panic(err)
		}
	}
	if p.round == p.Conf.ViewCount {
		p.LoadSchema(ctx)
		if err := p.Executor.ReloadSchema(); err != nil {
			panic(err)
		}
	}
	// log.Info("run one statement successfully!", zap.String("query", selectStmt))
}

func (p *Pivot) addExprIndex() {
	for i := 0; i < Rd(10)+1; i++ {
		n := p.createExpressionIndex()
		if n == nil {
			continue
		}
		var sql string
		if sql, err := BufferOut(n); err != nil {
			// should never panic
			panic(errors.Trace(err))
		} else if _, err = p.Executor.GetConn().Select(sql); err != nil {
			panic(errors.Trace(err))
		}
		fmt.Println("add one index on expression success SQL:" + sql)
	}
	fmt.Println("Create expression index successfully")
}

func (p *Pivot) createExpressionIndex() *ast.CreateIndexStmt {
	table := p.Tables[Rd(len(p.Tables))]
	/* only contains a primary key col and a varchar col in `table_varchar`
	   it will cause panic when create an expression index on it
	   since a varchar can not do logic ops and other ops with numberic
	*/
	if table.Name.EqString("table_varchar") {
		return nil
	}
	columns := make([]Column, 0)
	// remove auto increment column for avoiding ERROR 3109:
	// `Generated column '' cannot refer to auto-increment column`
	for _, column := range table.Columns {
		if !column.Name.HasPrefix("id_") {
			columns = append(columns, column)
		}
	}
	var backup []Column
	copy(backup, table.Columns)
	table.Columns = columns

	exprs := make([]ast.ExprNode, 0)
	for x := 0; x < Rd(3)+1; x++ {
		gCtx := generator.NewGenCtx([]Table{table}, nil)
		gCtx.IsInExprIndex = true
		gCtx.EnableLeftRightJoin = false
		exprs = append(exprs, &ast.ParenthesesExpr{Expr: p.WhereClauseAst(gCtx, 1)})
	}
	node := ast.CreateIndexStmt{}
	node.IndexName = "idx_" + RdStringChar(5)
	node.Table = &ast.TableName{Name: table.Name.ToModel()}
	node.IndexPartSpecifications = make([]*ast.IndexPartSpecification, 0)
	for _, expr := range exprs {
		node.IndexPartSpecifications = append(node.IndexPartSpecifications, &ast.IndexPartSpecification{
			Expr: expr,
		})
	}
	node.IndexOption = &ast.IndexOption{}

	table.Columns = backup
	return &node
}

func (p *Pivot) randTables() []Table {
	count := 1
	if len(p.Tables) > 1 {
		// avoid too deep joins
		if count = Rd(len(p.Tables)-1) + 1; count > 4 {
			count = Rd(4) + 1
		}
	}
	rand.Shuffle(len(p.Tables), func(i, j int) { p.Tables[i], p.Tables[j] = p.Tables[j], p.Tables[i] })
	usedTables := make([]Table, count)
	copy(usedTables, p.Tables[:count])
	return usedTables
}

// ChoosePivotedRow choose a row
// it may move to another struct
func (p *Pivot) ChoosePivotedRow() (map[string]*connection.QueryItem, []Table, error) {
	result := make(map[string]*connection.QueryItem)
	usedTables := p.randTables()
	var reallyUsed []Table

	for _, i := range usedTables {
		sql := fmt.Sprintf("SELECT * FROM %s ORDER BY RAND() LIMIT 1;", i.Name)
		exeRes, err := p.execSelect(sql)
		if err != nil {
			panic(err)
		}
		if len(exeRes) > 0 {
			for _, c := range exeRes[0] {
				// panic(fmt.Sprintf("no rows in table %s", i.Column))
				tableColumn := Column{Table: i.Name, Name: CIStr(c.ValType.Name())}
				result[tableColumn.String()] = c
			}
			reallyUsed = append(reallyUsed, i)

		}
	}
	return result, reallyUsed, nil
}

func (p *Pivot) GenSelectStmt(pivotRows map[string]*connection.QueryItem,
	usedTables []Table) (*ast.SelectStmt, string, []Column, map[string]*connection.QueryItem, *generator.GenCtx, error) {
	genCtx := generator.NewGenCtx(usedTables, pivotRows)
	stmtAst, err := p.SelectStmtAst(genCtx, p.Conf.Depth)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	sql, columns, updatedPivotRows, err := p.SelectStmt(&stmtAst, genCtx)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	return &stmtAst, sql, columns, updatedPivotRows, genCtx, nil
}

func (p *Pivot) ExecAndVerify(stmt *ast.SelectStmt, originRow map[string]*connection.QueryItem, columns []Column) (bool, error) {
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
	if p.Conf.Debug {
		fmt.Println("[DEBUG] SQL exec: " + stmt)
	}
	return p.Executor.GetConn().Select(stmt)
}

func (p *Pivot) verify(originRow map[string]*connection.QueryItem, columns []Column, resultSets [][]*connection.QueryItem) bool {
	for _, row := range resultSets {
		if p.checkRow(originRow, columns, row) {
			return true
		}
	}
	if p.Conf.Debug {
		fmt.Println("[DEBUG]")
		fmt.Println("  =========  ORIGIN ROWS ======")
		for k, v := range originRow {
			fmt.Printf("  key: %+v, value: [null: %v, value: %s]\n", k, v.Null, v.ValString)
		}

		fmt.Println("  =========  COLUMNS ======")
		for _, c := range columns {
			fmt.Printf("  Table: %s, Column: %s\n", c.Table, c.Name)
		}
	}
	return false
}

func (p *Pivot) checkRow(originRow map[string]*connection.QueryItem, columns []Column, resultSet []*connection.QueryItem) bool {
	for i, c := range columns {
		// fmt.Printf("i: %d, column: %+v, left: %+v, right: %+v", i, c, originRow[c], resultSet[i])
		if !compareQueryItem(originRow[c.GetAliasName().String()], resultSet[i]) {
			return false
		}
	}
	return true
}

func (p *Pivot) printPivotRows(pivotRows map[string]*connection.QueryItem) {
	var tableColumns Columns
	for column := range pivotRows {
		parsed := strings.Split(column, ".")
		table, col := parsed[0], parsed[1]
		tableColumns = append(tableColumns, Column{
			Table: CIStr(table),
			Name:  CIStr(col),
		})
	}

	sort.Sort(tableColumns)
	for _, column := range tableColumns {
		value := pivotRows[column.String()]
		fmt.Printf("%s.%s=%s\n", column.Table, column.Name, value.String())
	}
}

func compareQueryItem(left *connection.QueryItem, right *connection.QueryItem) bool {
	// if left.ValType.Name() != right.ValType.Name() {
	// 	return false
	// }
	if left.Null != right.Null {
		return false
	}

	return (left.Null && right.Null) || (left.ValString == right.ValString)
}

func (p *Pivot) refreshDatabase(ctx context.Context) {
	p.inWrite.Lock()
	defer func() {
		p.inWrite.Unlock()
	}()
	if p.Conf.Debug {
		fmt.Println("refresh database")
	}
	p.cleanup(ctx)
	p.prepare(ctx)
	if p.Conf.ExprIndex {
		p.addExprIndex()
		// reload indexes created
		p.LoadSchema(ctx)
	}
}

func (p *Pivot) GenNoRecNormalSelectStmt(usedTables []Table) (*ast.SelectStmt, string, error) {
	genCtx := generator.NewGenCtx(usedTables, nil)
	genCtx.IgnorePivotRow = true
	stmtAst, err := p.SelectStmtAst(genCtx, p.Conf.Depth)
	if err != nil {
		return nil, "", err
	}
	sql, _, _, err := p.SelectStmt(&stmtAst, genCtx)
	if err != nil {
		return nil, "", err
	}
	return &stmtAst, sql, nil
}

func (p *Pivot) GenNoRecSelectStmtNoOpt(usedTables []Table, node *ast.SelectStmt) (*ast.SelectStmt, string, error) {
	sum := &ast.SelectField{
		Expr: &ast.AggregateFuncExpr{
			F: "sum",
			Args: []ast.ExprNode{
				&ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: model.NewCIStr(NOREC_TMP_COL_NAME),
					},
				},
			},
		},
	}
	node.Fields.Fields = []*ast.SelectField{
		&ast.SelectField{
			Expr:   node.Where,
			AsName: model.NewCIStr(NOREC_TMP_COL_NAME),
		},
	}
	node.Where = nil
	// clear sql hint
	node.TableHints = make([]*ast.TableOptimizerHint, 0)
	wrapNode := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{
				sum,
			},
		},
		From: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableSource{
					AsName: model.NewCIStr(NOREC_TMP_TABLE_NAME),
					Source: node,
				},
			},
		},
	}
	sql, err := BufferOut(wrapNode)
	if err != nil {
		return nil, "", err
	}
	return wrapNode, sql, nil
}

func (p *Pivot) verifyNoREC(node *ast.SelectStmt, rows [][]*connection.QueryItem, usedTables []Table) bool {
	_, noOptSql, err := p.GenNoRecSelectStmtNoOpt(usedTables, node)
	if err != nil {
		panic(fmt.Sprintf("generate no opt SQL failed err:%+v", err))
	}
	resultRows, err := p.execSelect(noOptSql)
	if err != nil {
		panic(fmt.Sprintf("no opt sql executes failed: %+v", err))
	}
	rawLeft := rows[0][0].ValString
	if rawLeft == "NULL" || rawLeft == "" {
		rawLeft = "0"
	}
	rawRight := resultRows[0][0].ValString
	if rawRight == "NULL" || rawRight == "" {
		rawRight = "0"
	}
	fmt.Printf("rows: %+v resultRows: %+v", rawLeft, rawRight)
	left, err := strconv.ParseUint(rawLeft, 10, 64)
	if err != nil {
		panic("rows transform to int64 failed： " + rows[0][0].ValString)
	}
	right, err := strconv.ParseUint(rawRight, 10, 64)
	if err != nil {
		panic("resultRows transform to int64 failed： " + resultRows[0][0].ValString)
	}

	return right == left
}
