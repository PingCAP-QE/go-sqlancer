package generator

import (
	"testing"

	. "github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	parserdriver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, sql string) ast.Node {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		t.Fatalf("got %v", err)
	}
	sel := stmtNodes[0].(*ast.SelectStmt)
	return sel.Where
}

func isTrueValue(expr parserdriver.ValueExpr) bool {
	zero := parserdriver.ValueExpr{}
	zero.SetInt64(0)
	res, _ := expr.CompareDatum(&stmtctx.StatementContext{AllowInvalidDate: true, IgnoreTruncate: true}, &zero.Datum)
	return res == 1
}

func TestCase1(t *testing.T) {
	//CREATE TABLE t0(c0 TEXT(10));
	//INSERT INTO t0(c0) VALUES (1);
	//CREATE INDEX i0 ON t0(c0(10));
	//SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0; -- expected: {1}, actual: {}

	value := evaluateRow(parse(t, "SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "float", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): 1,
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

//func TestCase2(t *testing.T) {
//	//CREATE TABLE t0(c0 DOUBLE UNSIGNED UNIQUE);
//	//INSERT INTO t0(c0) VALUES (0);
//	//SELECT * FROM t0 WHERE t0.c0 = -1; -- expected: {}, actual: {0}
//	value := evaluateRow(parse(t, "SELECT * FROM t0 WHERE t0.c0 = -1"), []Table{{
//		Name:    CIStr("t0"),
//		Columns: [][3]string{{"c0", "double unsigned", "YES"}},
//		Indexes: nil,
//	}}, map[string]interface{}{
//		Column{Table: CIStr("t0"), Name: CIStr("c0")}: 1.0,
//	})
//	require.Equal(t, false, isTrueValue(value))
//}

func TestCase4(t *testing.T) {
	//CREATE TABLE t0(c0 NUMERIC PRIMARY KEY);
	//INSERT IGNORE INTO t0(c0) VALUES (NULL);
	//SELECT * FROM t0 WHERE c0; -- expected: {}, actual: {0}
	value := evaluateRow(parse(t, "SELECT * FROM t0 WHERE t0.c0"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "numeric", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): 0.0,
	}, make(map[string]string))
	require.Equal(t, false, isTrueValue(value))
}

func TestCase6(t *testing.T) {
	// CREATE TABLE t0(c0 CHAR AS (c1) UNIQUE, c1 INT);
	// INSERT INTO t0(c1) VALUES (0), (1);
	// SELECT * FROM t0; -- connection running loop panic
	value := evaluateRow(parse(t, "SELECT * FROM t0"), []Table{{
		Name: CIStr("t0"),
		Columns: []Column{
			{Name: "c0", Type: "char", Null: true},
			{Name: "t1", Type: "int"},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): "0",
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase8(t *testing.T) {
	// CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY);
	// INSERT INTO t0(c1) VALUES (0);
	// CREATE INDEX i0 ON t0(c0);
	// SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0;
	// SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0; -- expected: {1}, actual: {NULL}
	value := evaluateRow(parse(t, "SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0"), []Table{{
		Name: CIStr("t0"),
		Columns: []Column{
			{Name: "c0", Type: "int", Null: true},
			{Name: "t1", Type: "int"},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): 1,
		Column{Table: CIStr("t0"), Name: CIStr("c1")}.String(): 0,
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase11(t *testing.T) {
	//CREATE TABLE t0(c0 INT, c1 INT, PRIMARY KEY(c1));
	//CREATE INDEX i0 ON t0(c0);
	//SELECT /*+ USE_INDEX_MERGE(t0, PRIMARY) */ * FROM t0 WHERE 1 OR t0.c1;
	value := evaluateRow(parse(t, "SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0"), []Table{{
		Name: CIStr("t0"),
		Columns: []Column{
			{Name: "c0", Type: "int", Null: true},
			{Name: "t1", Type: "int"},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): 1,
		Column{Table: CIStr("t0"), Name: CIStr("c1")}.String(): 0,
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase12(t *testing.T) {
	//CREATE TABLE t0(c0 TEXT(10));
	//INSERT INTO t0(c0) VALUES (1);
	//CREATE INDEX i0 ON t0(c0(10));
	//SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0; -- expected: {1}, actual: {}
	value := evaluateRow(parse(t, "SELECT * FROM t0 WHERE ('a' != t0.c0) AND t0.c0"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "text", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): "1",
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase14(t *testing.T) {
	//CREATE TABLE t0(c0 FLOAT);
	//INSERT INTO t0(c0) VALUES (NULL);
	//SELECT * FROM t0 WHERE NOT(0 OR t0.c0); -- expected: {}, actual: {NULL}
	value := evaluateRow(parse(t, "SELECT * FROM t0 WHERE NOT(0 OR t0.c0)"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "float", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): nil,
	}, make(map[string]string))
	require.Equal(t, false, isTrueValue(value))
}

//func TestCase15(t *testing.T) {
//	//CREATE TABLE t0(c0 INT);
//	//INSERT INTO t0(c0) VALUES (0);
//	//SELECT t0.c0 FROM t0 WHERE CHAR(204355900); -- expected: {0}, actual: {}
//
//	value := evaluateRow(parse(t, "SELECT t0.c0 FROM t0 WHERE CHAR(204355900)"), []Table{{
//		Name:    CIStr("t0"),
//		Columns: [][3]string{{"c0", "int", "YES"}},
//		Indexes: nil,
//	}}, map[string]interface{}{
//		Column{Table: CIStr("t0"), Name: CIStr("c0")}: 0,
//	})
//	require.Equal(t, true, isTrueValue(value))
//}

func TestCase16(t *testing.T) {
	//CREATE TABLE t0(c0 INT);
	//CREATE VIEW v0(c0) AS SELECT 0 FROM t0 ORDER BY -t0.c0;
	//SELECT * FROM v0 RIGHT JOIN t0 ON false; -- connection running loop panic
}

//func TestCase22(t *testing.T) {
//	// CREATE TABLE t0(c0 FLOAT);
//	// CREATE TABLE t1(c0 FLOAT);
//	// INSERT INTO t1(c0) VALUES (0);
//	// INSERT INTO t0(c0) VALUES (0);
//	// SELECT t1.c0 FROM t1, t0 WHERE t0.c0=-t1.c0; -- expected: {0}, actual: {}
//	value := evaluateRow(parse(t, "SELECT t1.c0 FROM t1, t0 WHERE t0.c0=-t1.c0"), []Table{{
//		Name:    CIStr("t0"),
//		Columns: [][3]string{{"c0", "float", "YES"}},
//		Indexes: nil,
//	}, {
//		Name:    CIStr("t1"),
//		Columns: [][3]string{{"c0", "float", "YES"}},
//		Indexes: nil,
//	}}, map[string]interface{}{
//		Column{Table: CIStr("t0"), Name: CIStr("c0")}: 0.0,
//		Column{Table: CIStr("t1"), Name: CIStr("c0")}: 0.0,
//	})
//	require.Equal(t, true, isTrueValue(value))
//}

func TestCase29(t *testing.T) {
	//CREATE TABLE t0(c0 BOOL);
	//INSERT INTO t0 VALUES (0);
	//SELECT * FROM t0 WHERE 1 AND 0.4; -- expected: {0}, actual: {}
	value := evaluateRow(parse(t, "SELECT * FROM t0 WHERE 1 AND 0.4"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "bool", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): false,
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase30(t *testing.T) {
	// CREATE TABLE t0(c0 INT, c1 TEXT AS (0.9));
	// INSERT INTO t0(c0) VALUES (0);
	// SELECT 0 FROM t0 WHERE false UNION SELECT 0 FROM t0 WHERE NOT t0.c1; -- expected: {0}, actual: {}
}

func TestCase31(t *testing.T) {
	//CREATE TABLE t0(c0 INT);
	//INSERT INTO t0(c0) VALUES (2);
	//SELECT t0.c0 FROM t0 WHERE (NOT NOT t0.c0) = t0.c0; -- expected: {}, actual: {2}

	value := evaluateRow(parse(t, "SELECT t0.c0 FROM t0 WHERE (NOT NOT t0.c0) = t0.c0"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "int", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): 2,
	}, make(map[string]string))
	require.Equal(t, false, isTrueValue(value))
}

func TestCase_s01(t *testing.T) {
	value := evaluateRow(parse(t, "SELECT table_int_varchar_text.id,table_int_varchar_text.col_int,table_int_varchar_text.col_varchar,table_int_varchar_text.col_text,table_int_text.id,table_int_text.col_int,table_int_text.col_text FROM table_int_varchar_text JOIN table_int_text WHERE ((table_int_varchar_text.col_varchar!=-1) AND (table_int_varchar_text.col_text>=0e+00))"), []Table{{
		Name: CIStr("table_int_varchar_text"),
		Columns: []Column{
			{Name: "col_varchar", Type: "varchar", Null: true},
			{Name: "col_text", Type: "text", Null: true},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("table_int_varchar_text"), Name: CIStr("col_varchar")}.String(): 0,
		Column{Table: CIStr("table_int_varchar_text"), Name: CIStr("col_text")}.String():    nil,
	}, make(map[string]string))
	require.Equal(t, false, isTrueValue(value))
}

func TestCase_s02(t *testing.T) {
	value := evaluateRow(parse(t, "select * from t0 where !null is NULL"), []Table{{
		Name:    CIStr("t0"),
		Columns: []Column{{Name: "c0", Type: "int", Null: true}},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("t0"), Name: CIStr("c0")}.String(): 2,
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase_s03(t *testing.T) {
	value := evaluateRow(parse(t, `

SELECT table_int_varchar_float_text.id,
       table_int_varchar_float_text.col_int,
       table_int_varchar_float_text.col_varchar,
       table_int_varchar_float_text.col_float,
       table_int_varchar_float_text.col_text
FROM table_int_varchar_float_text
WHERE (((!table_int_varchar_float_text.id) XOR (-1
                                                AND table_int_varchar_float_text.col_float)))

`), []Table{{
		Name: CIStr("table_int_varchar_float_text"),
		Columns: []Column{
			{Name: "id", Type: "int", Null: true},
			{Name: "col_int", Type: "int", Null: true},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("table_inCIStrt_varchar_float_text"), Name: CIStr("id")}.String():   5,
		Column{Table: CIStr("table_int_varchar_float_text"), Name: CIStr("col_float")}.String(): nil,
	}, make(map[string]string))
	require.Equal(t, false, isTrueValue(value))
}

func TestCase_s04(t *testing.T) {
	// table_varchar_float_text.id:15
	// table_varchar_float_text.col_varchar:true
	// table_varchar_float_text.col_float:-0.1
	// table_varchar_float_text.col_text:-1
	// table_int.id:9
	// table_int.col_int:1
	value := evaluateRow(parse(t, `
SELECT table_varchar_float_text.id,
       table_varchar_float_text.col_varchar,
       table_varchar_float_text.col_float,
       table_varchar_float_text.col_text,
       table_int.id,
       table_int.col_int
FROM table_varchar_float_text
JOIN table_int
WHERE !((table_varchar_float_text.col_float XOR 15))
`), []Table{{
		Name: CIStr("table_varchar_float_text"),
		Columns: []Column{
			{Name: "id", Type: "int", Null: true},
			{Name: "col_varchar", Type: "varchar", Null: true},
			{Name: "col_float", Type: "float", Null: true},
			{Name: "col_text", Type: "text", Null: true},
		},
		Indexes: nil,
	}, {
		Name: CIStr("table_int"),
		Columns: []Column{
			{Name: "id", Type: "int", Null: true},
			{Name: "col_int", Type: "int", Null: true},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("table_varchar_float_text"), Name: CIStr("id")}.String():          15,
		Column{Table: CIStr("table_varchar_float_text"), Name: CIStr("col_varchar")}.String(): true,
		Column{Table: CIStr("table_varchar_float_text"), Name: CIStr("col_float")}.String():   -0.1,
		Column{Table: CIStr("table_varchar_float_text"), Name: CIStr("col_text")}.String():    -1,
		Column{Table: CIStr("table_int"), Name: CIStr("id")}.String():                         9,
		Column{Table: CIStr("table_int"), Name: CIStr("col_int")}.String():                    1,
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}

func TestCase_s05(t *testing.T) {
	//row:
	//	table_int_varchar_text.id:8
	//	table_int_varchar_text.col_int:
	//	table_int_varchar_text.col_varchar:
	//	table_int_varchar_text.col_text:0
	//	table_float.id:12
	//	table_float.col_float:
	//	table_int.id:11
	//	table_int.col_int:0
	//panic: data verified failed
	value := evaluateRow(parse(t, `
SELECT table_int_varchar_text.id,
       table_int_varchar_text.col_int,
       table_int_varchar_text.col_varchar,
       table_int_varchar_text.col_text,
       table_float.id,
       table_float.col_float,
       table_int.id,
       table_int.col_int
FROM (table_int_varchar_text
      JOIN table_float)
JOIN table_int
WHERE ((table_int_varchar_text.id XOR 4.0631823313220344e-01) XOR (!table_int_varchar_text.col_text))
`), []Table{{
		Name: CIStr("table_int_varchar_text"),
		Columns: []Column{
			{Name: "id", Type: "int", Null: true},
			{Name: "col_text", Type: "text", Null: true},
		},
		Indexes: nil,
	}}, map[string]interface{}{
		Column{Table: CIStr("table_int_varchar_text"), Name: CIStr("id")}.String():       8,
		Column{Table: CIStr("table_int_varchar_text"), Name: CIStr("col_text")}.String(): "0",
	}, make(map[string]string))
	require.Equal(t, true, isTrueValue(value))
}
