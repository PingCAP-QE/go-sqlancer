package equtrans

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	TestCases = [][]string{
		{"SELECT * FROM t WHERE t.c", "SELECT * FROM t WHERE t.c IS TRUE UNION ALL SELECT * FROM t WHERE t.c IS FALSE SELECT * FROM t WHERE t.c IS NULL"},
	}
)

func testTLPTrans_Trans(t *testing.T, parser *parser.Parser, input, expect string) {
	nodes, warns, err := parser.Parse(input, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)
	selectStmt, ok := nodes[0].(*ast.SelectStmt)
	assert.True(t, ok)
	assert.NotNil(t, selectStmt.Where)
	expr := selectStmt.Where
	selectStmt.Where = nil

	tlpTrans := TLPTrans{Expr: expr}
	output, err := util.BufferOut(tlpTrans.Trans(selectStmt))
	assert.Nil(t, err)

	expectNodes, warns, err := parser.Parse(expect, "", "")
	assert.Nil(t, err)
	assert.Empty(t, warns)
	assert.True(t, len(nodes) == 1)

	expect, err = util.BufferOut(expectNodes[0])
	assert.Nil(t, err)
	assert.Equal(t, expect, output)
}

func TestTLPTrans_Trans(t *testing.T) {
	parser := parser.New()
	for _, testCase := range TestCases {
		testTLPTrans_Trans(t, parser, testCase[0], testCase[1])
	}
}
