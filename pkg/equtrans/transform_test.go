package equtrans

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"testing"
)

func TestParse(_t *testing.T) {
	stmt, warns, err := parser.New().Parse("SELECT * FROM (SELECT * FROM t0, t2 UNION SELECT * FROM t0, t2) as tmp", "", "")
	fmt.Printf("%#x", warns)
	if err != nil {
		_ = fmt.Errorf("error: %s", err.Error())
	}
	fmt.Printf("%s", stmt[0])
}

func TestTrans(_t *testing.T) {
	Trans([]Transformer{NoRECTrans}, new(ast.SelectStmt), 1)
}
