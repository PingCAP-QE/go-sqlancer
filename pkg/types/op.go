package types

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/parser/opcode"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

// TODO: can it copy from tidb/types?
const (
	StringArg           = 1 << iota
	DatetimeAsStringArg = 1 << iota // datetime formatted string: "1990-10-11"
	NumberLikeStringArg = 1 << iota // "1.001" "42f_xa" start with number
	IntArg              = 1 << iota
	FloatArg            = 1 << iota
	DatetimeArg         = 1 << iota
	NullArg             = 1 << iota

	AnyArg = 0xFFFF
)

func NewAcceptTypeMap() *map[int]int {
	m := make(map[int]int)
	m[StringArg] = AnyArg
	m[DatetimeAsStringArg] = AnyArg
	m[NumberLikeStringArg] = AnyArg
	m[IntArg] = AnyArg
	m[FloatArg] = AnyArg
	m[DatetimeArg] = AnyArg
	m[NullArg] = AnyArg
	return &m
}

type Evaluable = func(...parser_driver.ValueExpr) (parser_driver.ValueExpr, error)

type OpFuncEval interface {
	GetMinArgs() int
	SetMinArgs(int)
	GetMaxArgs() int
	SetMaxArgs(int)

	GetName() string
	SetName(string)

	GetAcceptType(int, int) int
	SetAcceptType(int, int, int)

	Eval(...parser_driver.ValueExpr) (parser_driver.ValueExpr, error)
}

type OpFuncIndex map[string]OpFuncEval

func (idx *OpFuncIndex) Eval(name string, vals ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
	var e parser_driver.ValueExpr
	if ev, ok := (*idx)[name]; !ok {
		return e, fmt.Errorf("no such function or op with opcode: %s", name)
	} else {
		return ev.Eval(vals...)
	}
}

func (idx *OpFuncIndex) Add(o OpFuncEval) {
	(*idx)[o.GetName()] = o
}

func (idx *OpFuncIndex) Find(name string) OpFuncEval {
	return (*idx)[name]
}

func (idx *OpFuncIndex) Rand() OpFuncEval {
	rd := rand.Intn(len(*idx))
	var f OpFuncEval
	for _, f = range *idx {
		if rd <= 0 {
			return f
		}
		rd--
	}
	return f
}

type BaseOpFunc struct {
	// TODO: support 3 or more args limitation
	// [0] indicates first param; if args are infinite, last item will be duplicated
	// [0] is [StringArg] => [AnyArg^DatetimeArg]: first arg is string then datetime is invalid as second arg
	acceptType []map[int]int
	// min and max; -1 indicates infinite
	minArgs int
	maxArgs int
	name    string
	evalFn  Evaluable
}

func (o *BaseOpFunc) GetMinArgs() int {
	return o.minArgs
}

func (o *BaseOpFunc) SetMinArgs(m int) {
	o.minArgs = m
}

func (o *BaseOpFunc) GetMaxArgs() int {
	return o.maxArgs
}

func (o *BaseOpFunc) SetMaxArgs(m int) {
	o.maxArgs = m
}

func (o *BaseOpFunc) GetName() string {
	return o.name
}

func (o *BaseOpFunc) SetName(n string) {
	o.name = n
}

func (o *BaseOpFunc) GetAcceptType(pos, tp int) int {
	return o.acceptType[pos][tp]
}

func (o *BaseOpFunc) SetAcceptType(pos, tp, val int) {
	o.acceptType[pos][tp] = val
}

func (o *BaseOpFunc) InitAcceptType(length int) {
	o.acceptType = make([]map[int]int, length)
	for i := 0; i < length; i++ {
		o.acceptType[i] = *NewAcceptTypeMap()
	}
}

func (o *BaseOpFunc) SetEvalFn(fn Evaluable) {
	o.evalFn = fn
}

func (o *BaseOpFunc) Eval(vals ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
	return o.evalFn(vals...)
}

type Op struct {
	BaseOpFunc

	opcode opcode.Op
}

func (o *Op) GetOpcode() opcode.Op {
	return o.opcode
}

func (o *Op) SetOpcode(code opcode.Op) {
	o.opcode = code
	o.name = opcode.Ops[code]
}

func NewOp(code opcode.Op, min, max int, fn Evaluable) *Op {
	var o Op
	o.SetOpcode(code)
	o.SetMinArgs(min)
	o.SetMaxArgs(max)
	o.SetEvalFn(fn)
	o.InitAcceptType(2)
	return &o
}

type Fn struct {
	BaseOpFunc
}

func NewFn(name string, min, max int, fn Evaluable) *Fn {
	var f Fn
	f.SetName(name)
	f.SetMaxArgs(max)
	f.SetMinArgs(min)
	f.SetEvalFn(fn)
	f.InitAcceptType(2)
	return &f
}

type CastFn struct {
	BaseOpFunc

	castFnTp int // CAST() or CONVERT() etc.
	toTp     int // CAST(.. AS __TO_TP__)
}

func (c *CastFn) GetCastFnTp() int {
	return c.castFnTp
}

func (c *CastFn) SetCastFnTp(tp int) {
	c.castFnTp = tp
}

func (c *CastFn) GetToTp() int {
	return c.toTp
}

func (c *CastFn) SetToTp(tp int) {
	c.toTp = tp
}

// different behavior from other functions
func NewCastFn(castTp int, fn Evaluable) *CastFn {
	var f CastFn
	// TODO: caseFnTp to readable name
	f.SetName(fmt.Sprintf("%d", castTp))
	f.SetCastFnTp(castTp)
	f.SetMaxArgs(1)
	f.SetMinArgs(1)
	f.SetEvalFn(fn)
	f.InitAcceptType(1)
	return &f
}
