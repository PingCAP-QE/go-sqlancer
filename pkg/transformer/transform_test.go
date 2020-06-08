package transformer

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"testing"
)

type TestCase struct {
	fail           bool
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
