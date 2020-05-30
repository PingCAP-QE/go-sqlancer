package equtrans

import (
	"fmt"
	"github.com/pingcap/parser"
	"testing"
)

func TestTrans(_t *testing.T) {
	stmt, warns, err := parser.New().Parse("SELECT * FROM t0, t2 UNION SELECT * FROM t0, t2", "", "")
	fmt.Printf("%#x", warns)
	if err != nil {
		_ = fmt.Errorf("error: %s", err.Error())
	}
	fmt.Printf("%s", stmt[0])
}
