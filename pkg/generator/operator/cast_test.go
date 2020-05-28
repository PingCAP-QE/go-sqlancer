package operator

import (
	"fmt"
	"testing"

	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestCastSigned(t *testing.T) {
	type kase struct {
		values []interface{}
		expect interface{}
	}

	cases := []kase{
		{[]interface{}{1.0}, 1},
		{[]interface{}{-1}, int64(-1)},
		{[]interface{}{"-1"}, int64(-1)},
		{[]interface{}{"-1.1"}, int64(-1)},
		{[]interface{}{nil}, nil},
		{[]interface{}{"str"}, int64(0)},
	}

	for i, kase := range cases {
		var values []parser_driver.ValueExpr
		for _, value := range kase.values {
			v := parser_driver.ValueExpr{}
			v.SetValue(value)
			values = append(values, v)
		}
		actual, err := CastSigned.Eval(values...)
		expect := parser_driver.ValueExpr{}
		expect.SetValue(kase.expect)
		assert.NoError(t, err, "should not return error")
		assert.Equal(t, expect, actual, fmt.Sprintf("case %d failed", i))
	}
}
