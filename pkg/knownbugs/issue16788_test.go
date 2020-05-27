package knownbugs

import (
	"testing"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	tidb_types "github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, sql string) ast.Node {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		t.Fatalf("got %v", err)
	}
	sel := stmtNodes[0].(*ast.SelectStmt)
	return sel
}

func TestCase1(t *testing.T) {
	s := parse(t, "SELECT * FROM t0 where 2 between 1 and 3")
	sl := s.(*ast.SelectStmt)
	sl.Where.(*ast.BinaryOperationExpr).L.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).R.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	sl.Where.(*ast.BinaryOperationExpr).R.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	d := NewDustbin([]ast.Node{sl}, nil)
	require.Equal(t, issue16788(&d), false)
}

func TestCase2(t *testing.T) {
	s := parse(t, "SELECT * FROM t0 WHERE (0.5 = t0.c0) AND (5 > NULL AND t0.c1)")
	sl := s.(*ast.SelectStmt)
	sl.Where.(*ast.BinaryOperationExpr).L.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).R.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	d := NewDustbin([]ast.Node{sl}, nil)
	require.Equal(t, issue16788(&d), true)
}

func TestCase3(t *testing.T) {
	s := parse(t, "SELECT * FROM t0 LEFT JOIN t1 ON (0.5 != t0.c0) AND ((5 > NULL) AND (t1.c1 = 0.55)) WHERE 1 = 1")
	sl := s.(*ast.SelectStmt)
	sl.From.TableRefs.On.Expr.(*ast.BinaryOperationExpr).R.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).R.(*ast.ParenthesesExpr).Expr.(*ast.BinaryOperationExpr).L.SetType(tidb_types.NewFieldType(mysql.TypeInt24))
	d := NewDustbin([]ast.Node{sl}, nil)
	require.Equal(t, issue16788(&d), true)
}
