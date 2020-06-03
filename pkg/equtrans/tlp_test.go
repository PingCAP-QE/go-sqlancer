package equtrans

import (
	"fmt"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestCase struct {
	tp                   TLPType
	origin, expr, expect string
}

var (
	TestCases = []TestCase{
		{
			tp:     WHERE,
			origin: "SELECT * FROM t",
			expr:   "t.c",
			expect: "SELECT * FROM t WHERE t.c IS TRUE " +
				"UNION ALL SELECT * FROM t WHERE t.c IS FALSE " +
				"UNION ALL SELECT * FROM t WHERE t.c IS NULL",
		},
		{
			tp:     ON_CONDITION,
			origin: "SELECT * FROM t",
			expr:   "t.c",
			expect: "SELECT * FROM t",
		},
		{
			tp:     HAVING,
			origin: "SELECT * FROM t",
			expr:   "t.c",
			expect: "SELECT * FROM t",
		},
		{
			tp:     WHERE,
			origin: "SELECT * FROM t0 JOIN t1",
			expr:   "t0.c=t1.c",
			expect: "SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS TRUE " +
				"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS FALSE " +
				"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS NULL",
		},
		{
			tp:     ON_CONDITION,
			origin: "SELECT * FROM t0 JOIN t1",
			expr:   "t0.c=t1.c",
			expect: "SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS TRUE " +
				"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS FALSE " +
				"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS NULL",
		},
		{
			tp:     HAVING,
			origin: "SELECT * FROM t0 JOIN t1",
			expr:   "t0.c=t1.c",
			expect: "SELECT * FROM t0 JOIN t1",
		},
		{
			tp:     WHERE,
			origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
			expr:   "t0.c=t1.c",
			expect: "SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS TRUE GROUP BY t0.c " +
				"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS FALSE GROUP BY t0.c " +
				"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS NULL GROUP BY t0.c",
		},
		{
			tp:     ON_CONDITION,
			origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
			expr:   "t0.c=t1.c",
			expect: "SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS TRUE GROUP BY t0.c " +
				"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS FALSE GROUP BY t0.c " +
				"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS NULL GROUP BY t0.c",
		},
		{
			tp:     HAVING,
			origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
			expr:   "SUM(t0.c) > 1",
			expect: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS TRUE " +
				"UNION ALL SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS FALSE " +
				"UNION ALL SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS NULL",
		},
	}
)

func TLPTransTrans(t *testing.T, parser *parser.Parser, testCase TestCase) {
	nodes, warns, err := parser.Parse(testCase.origin, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)
	selectStmt, ok := nodes[0].(*ast.SelectStmt)
	assert.True(t, ok)

	exprNode, warns, err := parseExpr(parser, testCase.expr)
	assert.Nil(t, err)
	assert.Empty(t, warns)

	tlpTrans := TLPTrans{Expr: exprNode, Tp: testCase.tp}
	output, err := util.BufferOut(tlpTrans.Trans(selectStmt))
	assert.Nil(t, err)

	expectNodes, warns, err := parser.Parse(testCase.expect, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)

	expect, err := util.BufferOut(expectNodes[0])
	assert.Nil(t, err)
	assert.Equal(t, expect, output)
}

func TestTLPTrans_Trans(t *testing.T) {
	parser := parser.New()
	for _, testCase := range TestCases {
		TLPTransTrans(t, parser, testCase)
	}
}

func parseExpr(parser *parser.Parser, expr string) (node ast.ExprNode, warns []error, err error) {
	nodes, warns, err := parser.Parse(fmt.Sprintf("SELECT * FROM t WHERE %s", expr), "", "")
	if err != nil || len(warns) != 0 || len(nodes) == 0 {
		return
	}
	if stmt, ok := nodes[0].(*ast.SelectStmt); ok {
		node = stmt.Where
	}
	return
}
