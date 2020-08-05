package mutasql

import (
	"bytes"
	"testing"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func parseNode(sql string) ast.Node {
	p := parser.New()
	stmtNodes, _, _ := p.Parse(sql, "", "")
	return stmtNodes[0]
}

func stringifyNode(node ast.Node) string {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		panic(zap.Error(err)) // should never get error
	}
	return out.String()
}

func TestReplaceTableNameInNode(t *testing.T) {
	node := parseNode("SELECT s.c1,t.c2 FROM s LEFT JOIN t ON t.c0=\"1995\" WHERE t.c1=s.c3")

	m := make(map[string]string)
	m["s"] = "p"
	m["t"] = "q"
	n := replaceTableNameInNode(node, m)

	assert.Equal(t, stringifyNode(n), "SELECT p.c1,q.c2 FROM p LEFT JOIN q ON q.c0=\"1995\" WHERE q.c1=p.c3")
}

func TestReplaceTableName(t *testing.T) {
	node := parseNode("SELECT s.c1,t.c2 FROM s LEFT JOIN t ON t.c0=\"1995\" WHERE t.c1=s.c3")

	tc := TestCase{D: make([]*Dataset, 0), Q: node, Mutable: true}
	tc.D = append(tc.D, &Dataset{})
	tc.D[0].Table.Name = types.CIStr("t")
	tc.D[0].Table.Columns = append(tc.D[0].Table.Columns,
		types.Column{Name: types.CIStr("c0"), Table: types.CIStr("t"), Type: "TEXT", Length: 255, Null: false},
		types.Column{Name: types.CIStr("c1"), Table: types.CIStr("t"), Type: "TEXT", Length: 255, Null: true},
		types.Column{Name: types.CIStr("c2"), Table: types.CIStr("t"), Type: "TEXT", Length: 255, Null: true},
	)
	tc.D[0].After = append(tc.D[0].After, parseNode("INSERT INTO t VALUES(\"1995\", NULL, NULL)"))

	m := make(map[string]string)
	m["s"] = "p"
	m["t"] = "q"
	tc.ReplaceTableName(m)

	assert.Equal(t, stringifyNode(tc.Q), "SELECT p.c1,q.c2 FROM p LEFT JOIN q ON q.c0=\"1995\" WHERE q.c1=p.c3")
	assert.Equal(t, tc.D[0].Table.Name.String(), "q")
	assert.Equal(t, tc.D[0].Table.Columns[0].Table.String(), "q")
	assert.Equal(t, stringifyNode(tc.D[0].After[0]), "INSERT INTO q VALUES (\"1995\",NULL,NULL)")
}
