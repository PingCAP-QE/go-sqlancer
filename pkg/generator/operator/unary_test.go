package operator

import (
	"testing"

	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestNotCaseInt_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-1)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

func TestNotCaseInt_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

func TestNotCaseFloat_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(float64(0.000))
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

func TestNotCaseFloat_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(float64(0.0001))
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

// TODO: fix it
func TestNotCaseString_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("-0.0001")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

func TestNotCaseString_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("¡º¢∫≠")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

func TestNotCaseString_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}

func TestNotCaseNull_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}
