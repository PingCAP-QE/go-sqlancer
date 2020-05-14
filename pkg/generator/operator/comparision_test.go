package operator

import (
	"math"
	"testing"

	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestComparisionNull_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue("˜ƒf∫∫ß")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetNull()
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionNull_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0.00)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetNull()
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionInt_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0.00)
	b := parser_driver.ValueExpr{}
	b.SetValue(0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionInt_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(false)
	b := parser_driver.ValueExpr{}
	b.SetValue(0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionInt_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-1)
	b := parser_driver.ValueExpr{}
	b.SetValue(1)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionFloat_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-1.000000)
	b := parser_driver.ValueExpr{}
	b.SetValue("-1")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionFloat_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(1.000000)
	b := parser_driver.ValueExpr{}
	b.SetValue("1.0001")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionFloat_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(math.SmallestNonzeroFloat64)
	b := parser_driver.ValueExpr{}
	b.SetValue(0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionFloat_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(math.MaxFloat64)
	b := parser_driver.ValueExpr{}
	b.SetValue(-math.MaxFloat64)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionString_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("")
	b := parser_driver.ValueExpr{}
	b.SetValue("ªµ∆4634")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionString_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(1.23456)
	b := parser_driver.ValueExpr{}
	b.SetValue("ªµ∆4634")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestComparisionString_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("Sss")
	b := parser_driver.ValueExpr{}
	b.SetValue("ss")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Ne.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Eq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Le.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Ge.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(true)
	actual, err = Lt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	expected.SetValue(false)
	actual, err = Gt.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	actual, err = NULLEq.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestNotString_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("Sss")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	a.SetValue("12abc")
	expected.SetValue(false)
	actual, err = Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	a.SetValue(".3,wZ!")
	expected.SetValue(false)
	actual, err = Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	a.SetValue(".1")
	expected.SetValue(false)
	actual, err = Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	a.SetValue(".0000001e+00")
	expected.SetValue(false)
	actual, err = Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)

	a.SetValue(".000000e+00")
	expected.SetValue(true)
	actual, err = Not.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestISNull_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	actual, err := ISNULL.Eval(a)
	assert.NoError(t, err, "should not return error")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(1)
	assert.Equal(t, expected, actual)

	a.SetValue(1)
	expected.SetValue(0)
	actual, err = ISNULL.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)

	a.SetValue(false)
	expected.SetValue(0)
	actual, err = ISNULL.Eval(a)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, expected, actual)
}