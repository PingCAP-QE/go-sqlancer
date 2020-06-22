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
	stmt, warns, err := parser.New().Parse("SELECT MAX(c0) FROM (SELECT MAX(c) as c0 FROM t UNION ALL SELECT MAX(c) as c0 FROM t) as tmp", "", "")
	fmt.Printf("%#x", warns)
	if err != nil {
		_ = fmt.Errorf("error: %s", err.Error())
	}
	fmt.Printf("%#v", stmt[0].(*ast.SelectStmt).From.TableRefs.Left)
}
