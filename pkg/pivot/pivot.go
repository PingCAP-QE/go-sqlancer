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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/chaos-mesh/go-sqlancer/pkg/connection"
	"github.com/chaos-mesh/go-sqlancer/pkg/executor"
	"github.com/chaos-mesh/go-sqlancer/pkg/generator"
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
)

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

	// Warn: Hard code db name
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
			_, tables, err := p.ChoosePivotedRow(false)

			if err != nil {
				panic(errors.Trace(err))
			}
			if len(tables) == 0 {
				log.L().Panic("tables random by ChoosePivotedRow is empty")
			}
			var dmlStmt string
			switch Rd(2) {
			case 0:
				dmlStmt, err = p.GenerateDeleteDMLStmt(tables, *table)
				if err != nil {
					// TODO: goto next generation
					log.L().Error("generate delete stmt failed", zap.Error(err))
				}
			default:
				dmlStmt, err = p.GenerateUpdateDMLStmt(tables, *table)
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

// NOTICE: not support multi-table update
func (p *Pivot) GenerateUpdateDMLStmt(tables []types.Table, cTable Table) (string, error) {
	node := &ast.UpdateStmt{}
	for _, t := range tables {
		if t.Name.Eq(cTable.Name) {
			goto GCTX_UPDATE
		}
	}
	tables = append(tables, cTable)
GCTX_UPDATE:
	gCtx := generator.NewGenCtx(false, true, tables, nil)
	node.Where = p.WhereClauseAst(gCtx, 1)
	node.IgnoreErr = true
	node.TableRefs = &ast.TableRefsClause{TableRefs: &ast.Join{}}
	p.GenResultSetNode(node.TableRefs.TableRefs, gCtx)
	node.List = make([]*ast.Assignment, 0)
	for i := Rd(3) + 1; i > 0; i-- {
		asn := ast.Assignment{}
		// remove id col
		tmpCols := make(Columns, len(cTable.Columns))
		copy(tmpCols, cTable.Columns)
		cTable.Columns = make(Columns, 0)
		for _, c := range tmpCols {
			if !strings.HasPrefix(c.Name.String(), "id") {
				cTable.Columns = append(cTable.Columns, c)
			}
		}
		col := cTable.RandColumn()
		// restore cols
		cTable.Columns = tmpCols

		asn.Column = col.ToModel().Name
		argTp := TransStringType(col.Type)
		if RdBool() {
			asn.Expr = p.ConstValueExpr(argTp)
		} else {
			var err error
			asn.Expr, err = p.ColumnExpr(tables, argTp)
			if err != nil {
				log.L().Warn("columnExpr returns error", zap.Error(err))
				asn.Expr = p.ConstValueExpr(argTp)
			}
		}
		node.List = append(node.List, &asn)
	}
	// TODO: add hints

	if sql, err := BufferOut(node); err != nil {
		return "", errors.Trace(err)
	} else {
		return sql, nil
	}
}

// NOTICE: not support multi-table delete
func (p *Pivot) GenerateDeleteDMLStmt(tables []types.Table, cTable Table) (string, error) {
	node := &ast.DeleteStmt{}
	for _, t := range tables {
		if t.Name.Eq(cTable.Name) {
			goto GCTX_DELETE
		}
	}
	tables = append(tables, cTable)
GCTX_DELETE:
	gCtx := generator.NewGenCtx(false, true, tables, nil)
	node.Where = p.WhereClauseAst(gCtx, 1)
	node.IgnoreErr = true
	node.IsMultiTable = true
	node.BeforeFrom = true
	node.TableRefs = &ast.TableRefsClause{TableRefs: &ast.Join{}}
	p.GenResultSetNode(node.TableRefs.TableRefs, gCtx)
	// random some tables in UsedTables to be delete
	// deletedTables := tables[:RdRange(1, int64(len(tables)))]
	node.Tables = &ast.DeleteTableList{}
	node.Tables.Tables = make([]*ast.TableName, 0)
	node.Tables.Tables = append(node.Tables.Tables, &ast.TableName{Name: cTable.Name.ToModel()})
	// fmt.Printf("%+v", node.Tables.Tables[0])
	// for _, t := range deletedTables {
	// 	node.Tables.Tables = append(node.Tables.Tables, &ast.TableName{Name: t.Name.ToModel()})
	// }
	// TODO: add hint

	if sql, err := BufferOut(node); err != nil {
		return "", errors.Trace(err)
	} else {
		return sql, nil
	}
}

func (p *Pivot) cleanup(ctx context.Context) {
	_ = p.Executor.Exec("drop database if exists " + p.Conf.DBName)
	_ = p.Executor.Exec("create database " + p.Conf.DBName)
	_ = p.Executor.Exec("use " + p.Conf.DBName)
}

func (p *Pivot) kickup(ctx context.Context) {
	p.wg.Add(1)
	p.prepare(ctx)
	if p.Conf.ExprIndex {
		p.addExprIndex()
		// reload indexes created
		p.LoadSchema(ctx)
	}

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
						p.changeData(ctx)
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
	pivotRows, usedTables, err := p.ChoosePivotedRow(true)
	if err != nil {
		panic(err)
	}
	// generate sql ast tree and
	// generate sql where clause
	_, selectSQL, columns, updatedPivotRows, tableMap, err := p.GenSelectStmt(pivotRows, usedTables)
	if err != nil {
		panic(err)
	}
	// execute sql, ensure not null result set
	if RdBool() {
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
	// verify pivot row in result row set
	correct := p.verify(updatedPivotRows, columns, resultRows)
	fmt.Printf("Batch %d, Round %d, verify result %v!\n", p.batch, p.round, correct)
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
		if sql, err := BufferOut(n); err != nil {
			// should never panic
			panic(errors.Trace(err))
		} else if _, err = p.Executor.GetConn().Select(sql); err != nil {
			panic(errors.Trace(err))
		}
		fmt.Println("add one index on expression success")
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
		exprs = append(exprs, p.WhereClauseAst(generator.NewGenCtx(false, true, []Table{table}, nil), 1))
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

// ChoosePivotedRow choose a row
// it may move to another struct
func (p *Pivot) ChoosePivotedRow(skipEmpty bool) (map[string]*connection.QueryItem, []Table, error) {
	result := make(map[string]*connection.QueryItem)
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

	// for delete or update stmt which doesn't care pivot row
	if !skipEmpty {
		return nil, usedTables, nil
	}
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
	genCtx := generator.NewGenCtx(true, false, usedTables, pivotRows)
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

func (p *Pivot) minifySelect(stmt *ast.SelectStmt, pivotRows map[string]*connection.QueryItem, usedTable []Table, columns []Column, genCtx *generator.GenCtx) (string, error) {
	selectStmt := p.minifySubquery(stmt, stmt.Where, usedTable, pivotRows, columns)
	// for those ast that we don't try to simplify
	if selectStmt == nil {
		selectStmt = stmt
	}
	p.RectifyCondition(selectStmt.Where, genCtx)
	usedColumns := p.CollectColumnNames(selectStmt.Where)
	selectStmt.Fields.Fields = make([]*ast.SelectField, 0)
	for _, column := range usedColumns {
		name := column
		selectField := &ast.SelectField{
			Expr: &ast.ColumnNameExpr{
				Name: &name,
			},
		}
		selectStmt.Fields.Fields = append(selectStmt.Fields.Fields, selectField)
	}
	if len(selectStmt.Fields.Fields) == 0 {
		selectStmt.Fields.Fields = append(selectStmt.Fields.Fields, &ast.SelectField{
			Offset:   0,
			WildCard: &ast.WildCardField{},
		})
	}
	sql, err := BufferOut(selectStmt)
	if err != nil {
		return "", err
	}

	return sql, nil
}

func (p *Pivot) minifySubquery(stmt *ast.SelectStmt, e ast.Node, usedTable []Table, pivotRows map[string]*connection.QueryItem, columns []Column) *ast.SelectStmt {
	switch t := e.(type) {
	case *ast.ParenthesesExpr:
		result := p.minifySubquery(stmt, t.Expr, usedTable, pivotRows, columns)
		return result
	case *ast.BinaryOperationExpr:
		subSelectStmt := p.minifySubquery(stmt, t.L, usedTable, pivotRows, columns)
		if subSelectStmt != nil {
			return subSelectStmt
		}
		subSelectStmt = p.minifySubquery(stmt, t.R, usedTable, pivotRows, columns)
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
		// FIXME: tableMap
		if !p.verifyExistence(subSelectStmt, usedTable, pivotRows, exist, nil) {
			return subSelectStmt
		}
		return nil
	case *ast.UnaryOperationExpr:
		subSelectStmt := p.minifySubquery(stmt, t.V, usedTable, pivotRows, columns)
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
		// FIXME: table map
		if !p.verifyExistence(subSelectStmt, usedTable, pivotRows, exist, nil) {
			// pivotRows aren't in sub query stmt, we found a invalid sub query
			return subSelectStmt
		}
		return nil
	default:
		return nil
	}
}

func (p *Pivot) verifyExistence(sel *ast.SelectStmt, usedTables []Table, pivotRows map[string]*connection.QueryItem, exist bool, genCtx *generator.GenCtx) bool {
	where := sel.Where
	out := generator.Evaluate(where, genCtx)

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

func (p *Pivot) changeData(ctx context.Context) {
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
