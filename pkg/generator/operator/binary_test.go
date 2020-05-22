package operator

import (
	"math"
	"testing"

	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/stretchr/testify/assert"
)

func TestBinaryCaseNull_1(t *testing.T) {
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
}

func TestBinaryCaseNull_2(t *testing.T) {
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
}

func TestBinaryCaseInt_1(t *testing.T) {
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
}

func TestBinaryCaseInt_2(t *testing.T) {
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
}

func TestBinaryCaseInt_3(t *testing.T) {
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
}

func TestBinaryCaseFloat_1(t *testing.T) {
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
}

func TestBinaryCaseFloat_2(t *testing.T) {
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
}

func TestBinaryCaseFloat_3(t *testing.T) {
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
}

func TestBinaryCaseFloat_4(t *testing.T) {
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
}

func TestBinaryCaseString_1(t *testing.T) {
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
}

func TestBinaryCaseString_2(t *testing.T) {
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
}

func TestBinaryCaseString_3(t *testing.T) {
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

func TestLogicXorCaseNull_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(1)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseNull_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseNull_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue(-3.001)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseNull_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("cannot convert to float")
	b := parser_driver.ValueExpr{}
	b.SetValue(-3.001)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseFloat_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-1.01)
	b := parser_driver.ValueExpr{}
	b.SetValue(1.001)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseFloat_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0.000000e3)
	b := parser_driver.ValueExpr{}
	b.SetValue(1)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseFloat_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0.000000e3)
	b := parser_driver.ValueExpr{}
	b.SetValue(+1.01)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseFloat_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0.000000e3)
	b := parser_driver.ValueExpr{}
	b.SetValue("012")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseString_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("-0.000000e3")
	b := parser_driver.ValueExpr{}
	b.SetValue("+1E3")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseString_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("-0-0.000000e3")
	b := parser_driver.ValueExpr{}
	b.SetValue("0x13fa") // 0
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseString_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("_847x3&21)!(]3")
	b := parser_driver.ValueExpr{}
	b.SetValue("πß˜√œ≈øå˜çœ")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseString_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("")
	b := parser_driver.ValueExpr{}
	b.SetValue("")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicXorCaseInt_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0)
	b := parser_driver.ValueExpr{}
	b.SetValue(-65536)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicXor.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseInt_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0)
	b := parser_driver.ValueExpr{}
	b.SetValue(114514)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseInt_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0)
	b := parser_driver.ValueExpr{}
	b.SetValue(0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseNull_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(-0)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseNull_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue(-1.0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseNull_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseNull_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("•∞")
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseFloat_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("•∞")
	b := parser_driver.ValueExpr{}
	b.SetValue(-0.00001)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseFloat_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(0.0000)
	b := parser_driver.ValueExpr{}
	b.SetValue(-1.00001E10)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseFloat_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(0.0000)
	b := parser_driver.ValueExpr{}
	b.SetValue(-0.000E10)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseString_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("0.000")
	b := parser_driver.ValueExpr{}
	b.SetValue("0")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseString_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("0.000E1")
	b := parser_driver.ValueExpr{}
	b.SetValue("false")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

// TODO: fix it
// convert "0.01" to bool is not correct
func TestLogicOrCaseString_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue("0.00001")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseString_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue("")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicOrCaseString_5(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("")
	b := parser_driver.ValueExpr{}
	b.SetValue("")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicOr.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseNull_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue("0.00001")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseNull_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("sss")
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseNull_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(0)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseNull_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(nil)
	b := parser_driver.ValueExpr{}
	b.SetValue(nil)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(nil)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseString_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(0)
	b := parser_driver.ValueExpr{}
	b.SetValue("01")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseString_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("null")
	b := parser_driver.ValueExpr{}
	b.SetValue("1")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

// TODO: fix it
func TestLogicAndCaseString_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("-0.0001")
	b := parser_driver.ValueExpr{}
	b.SetValue(1.0000)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseString_4(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue("")
	b := parser_driver.ValueExpr{}
	b.SetValue(1.0000)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseFloat_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(1.00001E2)
	b := parser_driver.ValueExpr{}
	b.SetValue("-1.001")
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseFloat_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(float64(0.000))
	b := parser_driver.ValueExpr{}
	b.SetValue(-1.001)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseInt_1(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(0)
	b := parser_driver.ValueExpr{}
	b.SetValue(-0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseInt_2(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(1)
	b := parser_driver.ValueExpr{}
	b.SetValue(-1)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(true)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}

func TestLogicAndCaseInt_3(t *testing.T) {
	a := parser_driver.ValueExpr{}
	a.SetValue(1)
	b := parser_driver.ValueExpr{}
	b.SetValue(-0)
	expected := parser_driver.ValueExpr{}
	expected.SetValue(false)
	actual, err := LogicAnd.Eval(a, b)
	assert.NoError(t, err, "should not return error")
	assert.Equal(t, util.CompareValue(actual, expected), true)
}
