package sqlancer

import (
	"context"
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

const noRECTmpTableName = "tmp_table"
const noRECTmpColName = "tmp_col"

var (
	allColumnTypes = []string{"int", "float", "varchar"}
)

type checkMode = int

const (
	modePQS checkMode = iota
	modeNoREC
	modeTLP
)

type SQLancer struct {
	generator.Generator
	conf     *Config
	executor *executor.Executor

	inWrite      sync.RWMutex
	batch        int
	roundInBatch int
}

// NewSQLancer ...
func NewSQLancer(conf *Config) (*SQLancer, error) {
	e, err := executor.New(conf.DSN, conf.DBName)
	if err != nil {
		return nil, err
	}
	return &SQLancer{
		conf:      conf,
		executor:  e,
		Generator: generator.Generator{Config: generator.Config{Hint: conf.EnableHint}},
	}, nil
}

// Start SQLancer
func (p *SQLancer) Start(ctx context.Context) {
	p.run(ctx)
	p.tearDown()
}

func (p *SQLancer) tearDown() {
	p.executor.Close()
}

// LoadSchema load table/view/index schema
func (p *SQLancer) LoadSchema() {
	rand.Seed(time.Now().UnixNano())
	p.Tables = make([]Table, 0)

	tables, err := p.executor.GetConn().FetchTables(p.conf.DBName)
	if err != nil {
		panic(err)
	}
	for _, i := range tables {
		t := Table{Name: CIStr(i)}
		columns, err := p.executor.GetConn().FetchColumns(p.conf.DBName, i)
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
		idx, err := p.executor.GetConn().FetchIndexes(p.conf.DBName, i)
		if err != nil {
			panic(err)
		}
		for _, j := range idx {
			t.Indexes = append(t.Indexes, CIStr(j))
		}
		p.Tables = append(p.Tables, t)
	}
}

// setUpDB clears dirty data, creates db, table and populates data
func (p *SQLancer) setUpDB(ctx context.Context) {
	_ = p.executor.Exec("drop database if exists " + p.conf.DBName)
	_ = p.executor.Exec("create database " + p.conf.DBName)
	_ = p.executor.Exec("use " + p.conf.DBName)

	p.createSchema(ctx)
	p.populateData()
	p.createExprIdx()
}

func (p *SQLancer) createSchema(ctx context.Context) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	g, _ := errgroup.WithContext(ctx)
	for index, columnTypes := range ComposeAllColumnTypes(-1, allColumnTypes) {
		tableIndex := index
		colTs := make([]string, len(columnTypes))
		copy(colTs, columnTypes)
		g.Go(func() error {
			sql, _ := p.executor.GenerateDDLCreateTable(tableIndex, colTs)
			return p.executor.Exec(sql.SQLStmt)
		})
	}
	if err := g.Wait(); err != nil {
		log.L().Error("create table failed", zap.Error(err))
	}

	err := p.executor.ReloadSchema()
	if err != nil {
		log.Error("reload data failed!")
	}
	for i := 0; i < r.Intn(10); i++ {
		sql, err := p.executor.GenerateDDLCreateIndex()
		if err != nil {
			fmt.Println(err)
		}
		err = p.executor.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("create index failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
		}
	}
	p.LoadSchema()
}

func (p *SQLancer) populateData() {
	var err error
	if err := p.executor.GetConn().Begin(); err != nil {
		log.L().Error("begin txn failed", zap.Error(err))
		return
	}
	for _, table := range p.executor.GetTables() {
		insertData := func() {
			sql, err := p.executor.GenerateDMLInsertByTable(table.Name.String())
			if err != nil {
				panic(errors.ErrorStack(err))
			}
			err = p.executor.Exec(sql.SQLStmt)
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
			err = p.executor.Exec(dmlStmt)
			if err != nil {
				log.L().Error("update/delete data failed", zap.String("sql", dmlStmt), zap.Error(err))
				panic(err)
			}
		}

		countSQL := "select count(*) from " + table.Name.String()
		qi, err := p.executor.GetConn().Select(countSQL)
		if err != nil {
			log.L().Error("insert data failed", zap.String("sql", countSQL), zap.Error(err))
		}
		count := qi[0][0].ValString
		if p.conf.Debug {
			log.L().Debug("table check records count", zap.String(table.Name.String(), count))
		}

		if c, _ := strconv.ParseUint(count, 10, 64); c == 0 {
			log.L().Info(table.Name.String() + " is empty after DELETE")
			insertData()
		}
	}
	if err := p.executor.GetConn().Commit(); err != nil {
		log.L().Error("commit txn failed", zap.Error(err))
		return
	}
}

func (p *SQLancer) createExprIdx() {
	if p.conf.EnableExprIndex {
		p.addExprIndex()
		// reload indexes created
		p.LoadSchema()
	}
}

func (p *SQLancer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if p.roundInBatch == 0 {
				p.refreshDatabase(ctx)
				p.batch++
			}
			p.progress(ctx)
			p.roundInBatch = (p.roundInBatch + 1) % 100
		}
	}
}

func (p *SQLancer) progress(ctx context.Context) {
	p.inWrite.RLock()
	defer func() {
		p.inWrite.RUnlock()
	}()
	var modes []checkMode
	// Because we creates view just in time with process(we creates views on first TotalViewCount rounds)
	// and current implementation depends on the PQS to ensures that there exists at lease one row in that view
	// so we must choose modePQS in this scenario
	if p.roundInBatch < p.conf.TotalViewCount {
		modes = []checkMode{modePQS}
	} else {
		if p.conf.EnableNoRECMode {
			modes = append(modes, modeNoREC)
		}
		if p.conf.EnablePQSMode {
			modes = append(modes, modePQS)
		}
	}
	mode := modes[Rd(len(modes))]
	switch mode {
	case modePQS:
		// rand one pivot row for one table
		pivotRows, usedTables, err := p.ChoosePivotedRow()
		if err != nil {
			log.L().Fatal("choose pivot row failed", zap.Error(err))
		}
		selectAst, selectSQL, columns, updatedPivotRows, tableMap, err := p.GenSelectStmt(pivotRows, usedTables)
		p.withTxn(func() error {
			resultRows, err := p.execSelect(selectSQL)
			if err != nil {
				log.L().Error("execSelect failed", zap.Error(err))
				return err
			}
			correct := p.verify(updatedPivotRows, columns, resultRows)
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
				dust := knownbugs.NewDustbin([]ast.Node{selectAst}, pivotRows)
				if dust.IsKnownBug() {
					return nil
				}
				fmt.Printf("row:\n")
				p.printPivotRows(pivotRows)
				if p.roundInBatch < p.conf.TotalViewCount || p.conf.Silent {
					panic("data verified failed")
				}
				return nil
			}
			if p.roundInBatch <= p.conf.TotalViewCount {
				if err := p.executor.GetConn().CreateViewBySelect(fmt.Sprintf("view_%d", p.roundInBatch), selectSQL, len(resultRows), columns); err != nil {
					fmt.Println("create view failed", tableMap)
					panic(err)
				}
			}
			if p.roundInBatch == p.conf.TotalViewCount {
				p.LoadSchema()
				if err := p.executor.ReloadSchema(); err != nil {
					panic(err)
				}
			}
			log.L().Info("check finished", zap.String("mode", "PQS"), zap.Int("batch", p.batch), zap.Int("round", p.roundInBatch), zap.Bool("result", correct))
			return nil
		})
	case modeNoREC:
		selectAst, selectSQL, err := p.GenNoRecNormalSelectStmt()
		if err != nil {
			log.L().Error("generate NoREC SQL statement failed", zap.Error(err))
		}
		p.withTxn(func() error {
			resultRows, err := p.execSelect(selectSQL)
			if err != nil {
				log.L().Error("execSelect failed", zap.Error(err))
				return err
			}
			correct := p.verifyNoREC(selectAst, resultRows)
			if !correct {
				if !p.conf.Silent {
					log.L().Fatal("data verified failed")
				}
			}
			log.L().Info("check finished", zap.String("mode", "NoREC"), zap.Int("batch", p.batch), zap.Int("round", p.roundInBatch), zap.Bool("result", correct))
			return nil
		})
	default:
		log.L().Fatal("unknown check mode", zap.Int("mode", mode))
	}
}

func (p *SQLancer) withTxn(do func() error) error {
	var err error
	// execute sql, ensure not null result set
	if explicitTxn := RdBool(); explicitTxn {
		if err = p.executor.GetConn().Begin(); err != nil {
			log.L().Error("begin txn failed", zap.Error(err))
			return err
		}
		if p.conf.Debug {
			log.L().Debug("begin txn success")
		}
		defer func() {
			if err = p.executor.GetConn().Commit(); err != nil {
				log.L().Error("commit txn failed", zap.Error(err))
				return
			}
			if p.conf.Debug {
				log.L().Debug("commit txn success")
			}
		}()
	}
	return do()
}

func (p *SQLancer) addExprIndex() {
	for i := 0; i < Rd(10)+1; i++ {
		n := p.createExpressionIndex()
		if n == nil {
			continue
		}
		var sql string
		if sql, err := BufferOut(n); err != nil {
			// should never panic
			panic(errors.Trace(err))
		} else if _, err = p.executor.GetConn().Select(sql); err != nil {
			panic(errors.Trace(err))
		}
		fmt.Println("add one index on expression success SQL:" + sql)
	}
	fmt.Println("Create expression index successfully")
}

func (p *SQLancer) createExpressionIndex() *ast.CreateIndexStmt {
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

func (p *SQLancer) randTables() []Table {
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
func (p *SQLancer) ChoosePivotedRow() (map[string]*connection.QueryItem, []Table, error) {
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

func (p *SQLancer) GenSelectStmt(pivotRows map[string]*connection.QueryItem,
	usedTables []Table) (*ast.SelectStmt, string, []Column, map[string]*connection.QueryItem, *generator.GenCtx, error) {
	genCtx := generator.NewGenCtx(usedTables, pivotRows)
	stmtAst, err := p.SelectStmtAst(genCtx, p.conf.Depth)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	sql, columns, updatedPivotRows, err := p.SelectStmt(&stmtAst, genCtx)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	return &stmtAst, sql, columns, updatedPivotRows, genCtx, nil
}

func (p *SQLancer) ExecAndVerify(stmt *ast.SelectStmt, originRow map[string]*connection.QueryItem, columns []Column) (bool, error) {
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
func (p *SQLancer) execSelect(stmt string) ([][]*connection.QueryItem, error) {
	if p.conf.Debug {
		fmt.Println("[DEBUG] SQL exec: " + stmt)
	}
	return p.executor.GetConn().Select(stmt)
}

func (p *SQLancer) verify(originRow map[string]*connection.QueryItem, columns []Column, resultSets [][]*connection.QueryItem) bool {
	for _, row := range resultSets {
		if p.checkRow(originRow, columns, row) {
			return true
		}
	}
	if p.conf.Debug {
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

func (p *SQLancer) checkRow(originRow map[string]*connection.QueryItem, columns []Column, resultSet []*connection.QueryItem) bool {
	for i, c := range columns {
		// fmt.Printf("i: %d, column: %+v, left: %+v, right: %+v", i, c, originRow[c], resultSet[i])
		if !compareQueryItem(originRow[c.GetAliasName().String()], resultSet[i]) {
			return false
		}
	}
	return true
}

func (p *SQLancer) printPivotRows(pivotRows map[string]*connection.QueryItem) {
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

func (p *SQLancer) refreshDatabase(ctx context.Context) {
	p.inWrite.Lock()
	defer func() {
		p.inWrite.Unlock()
	}()
	if p.conf.Debug {
		fmt.Println("refresh database")
	}
	p.setUpDB(ctx)

}

func (p *SQLancer) GenNoRecNormalSelectStmt() (*ast.SelectStmt, string, error) {
	genCtx := generator.NewGenCtx(p.randTables(), nil)
	genCtx.IgnorePivotRow = true
	stmtAst, err := p.SelectStmtAst(genCtx, p.conf.Depth)
	if err != nil {
		return nil, "", err
	}
	sql, _, _, err := p.SelectStmt(&stmtAst, genCtx)
	if err != nil {
		return nil, "", err
	}
	return &stmtAst, sql, nil
}

func (p *SQLancer) GenNoRecSelectStmtNoOpt(node *ast.SelectStmt) (*ast.SelectStmt, string, error) {
	sum := &ast.SelectField{
		Expr: &ast.AggregateFuncExpr{
			F: "sum",
			Args: []ast.ExprNode{
				&ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: model.NewCIStr(noRECTmpColName),
					},
				},
			},
		},
	}
	node.Fields.Fields = []*ast.SelectField{
		&ast.SelectField{
			Expr:   node.Where,
			AsName: model.NewCIStr(noRECTmpColName),
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
					AsName: model.NewCIStr(noRECTmpTableName),
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

func (p *SQLancer) verifyNoREC(node *ast.SelectStmt, rows [][]*connection.QueryItem) bool {
	_, noOptSql, err := p.GenNoRecSelectStmtNoOpt(node)
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
	fmt.Printf("rows: %+v resultRows: %+v\n", rawLeft, rawRight)
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
