package transformer

import (
	"fmt"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

type TLPTestCase struct {
	TestCase
	tp   TLPType
	expr string
}

var (
	TLPTestCases = []TLPTestCase{
		{
			tp:   WHERE,
			expr: "t.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t",
				expect: "SELECT * FROM t WHERE t.c IS TRUE " +
					"UNION ALL SELECT * FROM t WHERE t.c IS FALSE " +
					"UNION ALL SELECT * FROM t WHERE t.c IS NULL",
			},
		},
		{
			tp:   ON_CONDITION,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM t",
				expect: "",
			},
		},
		{
			tp:   HAVING,
			expr: "t.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM t",
				expect: "",
			},
		},
		{
			tp:   WHERE,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1",
				expect: "SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS TRUE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS FALSE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS NULL",
			},
		},
		{
			tp:   ON_CONDITION,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1",
				expect: "SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS TRUE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS FALSE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS NULL",
			},
		},
		{
			tp:   HAVING,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				fail:   true,
				origin: "SELECT * FROM t0 JOIN t1",
				expect: "",
			},
		},
		{
			tp:   WHERE,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
				expect: "SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS TRUE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS FALSE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 WHERE t0.c=t1.c IS NULL GROUP BY t0.c",
			},
		},
		{
			tp:   ON_CONDITION,
			expr: "t0.c=t1.c",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
				expect: "SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS TRUE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS FALSE GROUP BY t0.c " +
					"UNION ALL SELECT * FROM t0 JOIN t1 ON t0.c=t1.c IS NULL GROUP BY t0.c",
			},
		},
		{
			tp:   HAVING,
			expr: "SUM(t0.c) > 1",
			TestCase: TestCase{
				origin: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c",
				expect: "SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS TRUE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS FALSE " +
					"UNION ALL SELECT * FROM t0 JOIN t1 GROUP BY t0.c HAVING SUM(t0.c) > 1 IS NULL",
			},
		},
	}
)

func TLPTransTest(t *testing.T, parser *parser.Parser, testCase TLPTestCase) {
	exprNode, warns, err := parseExpr(parser, testCase.expr)
	assert.Nil(t, err)
	assert.Empty(t, warns)
	tlpTrans := &TLPTrans{Expr: exprNode, Tp: testCase.tp}
	nodes, warns, err := parser.Parse(testCase.origin, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)
	selectStmt, ok := nodes[0].(*ast.SelectStmt)
	assert.True(t, ok)
	resultSetNodes := tlpTrans.Transform([][]ast.ResultSetNode{{selectStmt}})
	assert.NotEmpty(t, resultSetNodes)

	if testCase.fail {
		assert.True(t, assert.True(t, len(resultSetNodes[0]) == 1))
	} else {
		assert.True(t, len(resultSetNodes[0]) >= 2)
		output, err := util.BufferOut(resultSetNodes[0][1])
		assert.Nil(t, err)

		expectNodes, warns, err := parser.Parse(testCase.expect, "", "")
		assert.Nil(t, err)
		assert.Empty(t, warns)
		assert.True(t, len(nodes) == 1)

		expect, err := util.BufferOut(expectNodes[0])
		assert.Nil(t, err)
		assert.Equal(t, expect, output)
	}
}

func TestTLPTrans_Trans(t *testing.T) {
	parser := parser.New()
	for _, testCase := range TLPTestCases {
		TLPTransTest(t, parser, testCase)
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
