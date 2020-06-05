package transformer

import (
	"fmt"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

type TestCase struct {
	origin, expect string
}

func TestParse(_t *testing.T) {
	stmt, warns, err := parser.New().Parse("SELECT * FROM t0, t2 UNION SELECT * FROM t0, t2 UNION ALL SELECT * FROM t0, t2", "", "")
	fmt.Printf("%#x", warns)
	if err != nil {
		_ = fmt.Errorf("error: %s", err.Error())
	}
	fmt.Printf("%#v", stmt[0].(*ast.UnionStmt).SelectList.Selects[0])
}

func TransTest(t *testing.T, parser *parser.Parser, testCase TestCase, transformer Transformer) {
	nodes, warns, err := parser.Parse(testCase.origin, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)
	selectStmt, ok := nodes[0].(*ast.SelectStmt)
	assert.True(t, ok)

	output, err := util.BufferOut(transformer.Trans(selectStmt))
	assert.Nil(t, err)

	expectNodes, warns, err := parser.Parse(testCase.expect, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)

	expect, err := util.BufferOut(expectNodes[0])
	assert.Nil(t, err)
	assert.Equal(t, expect, output)
}

func TestTLPTrans(t *testing.T) {
	parser := parser.New()
	for _, testCase := range TLPTestCases {
		exprNode, warns, err := parseExpr(parser, testCase.expr)
		assert.Nil(t, err)
		assert.Empty(t, warns)
		tlpTrans := &TLPTrans{Expr: exprNode, Tp: testCase.tp}
		TransTest(t, parser, testCase.TestCase, (TransformFunc)(func(stmt *ast.SelectStmt) ast.ResultSetNode {
			return Trans([]Transformer{tlpTrans}, stmt, 1)[0]
		}))
	}
}
