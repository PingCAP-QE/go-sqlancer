package types

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

type Evaluable = func(...parser_driver.ValueExpr) (parser_driver.ValueExpr, error)
type GenNodeCb = func(uint64) (ast.ExprNode, parser_driver.ValueExpr, error)
type FnGenNodeCb = func(GenNodeCb, OpFuncEval, uint64) (ast.ExprNode, parser_driver.ValueExpr, error)

/* return 0, nil for error typed inputs; 0, error for other error (e.g. arg amount)
non-zero, nil for legal input; non-zero, error("warning") for warning
*/
type ValidateCb = func(...uint64) (uint64, bool, error)

type OpFuncEval interface {
	GetMinArgs() int
	SetMinArgs(int)
	GetMaxArgs() int
	SetMaxArgs(int)

	GetName() string
	SetName(string)

	MakeArgTable(bool)
	GetArgTable() ArgTable

	GetPossibleReturnType() uint64
	// IsValidParam(...uint64) (uint64, error)
	Eval(...parser_driver.ValueExpr) (parser_driver.ValueExpr, error)
	// for generate node
	Node(GenNodeCb, uint64) (ast.ExprNode, parser_driver.ValueExpr, error)
}

// return type as key; f => str|int : [TypeStr]:['f':f], [TypeInt]:['f':f]
type OpFuncIndex map[uint64]map[string]OpFuncEval

func (idx *OpFuncIndex) RandOpFn(tp uint64) (OpFuncEval, error) {
	if m, ok := (*idx)[tp]; !ok || len(m) == 0 {
		return nil, errors.New(fmt.Sprintf("no operations or functions return type: %d", tp))
	} else {
		keys := make([]string, 0)
		for k := range m {
			keys = append(keys, k)
		}
		return m[keys[rand.Intn(len(keys))]], nil
	}
}

type OpFuncMap map[string]OpFuncEval

func (m *OpFuncMap) Eval(name string, vals ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
	var e parser_driver.ValueExpr
	if ev, ok := (*m)[name]; !ok {
		return e, fmt.Errorf("no such function or op with opcode: %s", name)
	} else {
		return ev.Eval(vals...)
	}
}

func (m *OpFuncMap) Add(o OpFuncEval) {
	(*m)[o.GetName()] = o
}

func (m *OpFuncMap) Find(name string) OpFuncEval {
	return (*m)[name]
}

type BaseOpFunc struct {
	// min and max; -1 indicates infinite
	minArgs       int
	maxArgs       int
	name          string
	evalFn        Evaluable
	nodeFn        FnGenNodeCb
	validateParam ValidateCb
	argTable      ArgTable
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

func (o *BaseOpFunc) SetEvalFn(fn Evaluable) {
	o.evalFn = fn
}

func (o *BaseOpFunc) Eval(vals ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
	return o.evalFn(vals...)
}

func (o *BaseOpFunc) SetNodeFn(fn FnGenNodeCb) {
	o.nodeFn = fn
}

func (o *BaseOpFunc) Node(fn GenNodeCb, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
	// ? we can make o as a param
	// ? so that we can downcast o to Op/Fn/CastFn to call their owned methods
	return o.nodeFn(fn, o, ret)
}

func (o *BaseOpFunc) SetIsValidParam(fn ValidateCb) {
	o.validateParam = fn
}

func (o *BaseOpFunc) MakeArgTable(ignoreWarn bool) {
	o.argTable = NewArgTable(o.maxArgs)
	if o.maxArgs == 0 {
		// such as NOW()
		ret, warn, err := o.validateParam()
		if err != nil || warn {
			panic(fmt.Sprintf("call IsValidParam failed, err: %+v warn: %v", err, warn))
		}
		for ret != 0 {
			i := ret &^ (ret - 1)
			o.argTable.Insert(i)
			ret = ret & (ret - 1)
		}
	} else {
		stack := make([][]uint64, 0)
		for _, i := range SupportArgs {
			stack = append(stack, []uint64{i})
		}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if len(cur) == o.maxArgs {
				ret, warn, err := o.validateParam(cur...)
				if ret != 0 && err == nil {
					if ignoreWarn || !warn {
						for ret != 0 {
							i := ret &^ (ret - 1)
							o.argTable.Insert(i, cur...)
							ret = ret & (ret - 1)
						}
					}
				}
			} else {
				for _, i := range SupportArgs {
					stack = append(stack, append(cur, i))
				}
			}
		}
	}
}

func (o *BaseOpFunc) GetPossibleReturnType() (returnType uint64) {
	for _, i := range SupportArgs {
		args := make([]*uint64, 0)
		for j := 0; j < o.maxArgs; j++ {
			args = append(args, nil)
		}
		tmp := i
		res, err := o.argTable.Filter(args, &tmp)
		if err == nil && len(res) != 0 {
			returnType |= i
		}
	}
	return
}

func (o *BaseOpFunc) GetArgTable() ArgTable {
	return o.argTable
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
	o.name = code.String()
}

func NewOp(code opcode.Op, min, max int, fn Evaluable, vp ValidateCb, gn FnGenNodeCb) *Op {
	var o Op
	o.SetOpcode(code)
	o.SetMinArgs(min)
	o.SetMaxArgs(max)
	o.SetEvalFn(fn)
	o.SetIsValidParam(vp)
	o.SetNodeFn(gn)
	// TODO: give a context
	o.MakeArgTable(true)
	return &o
}

type Fn struct {
	BaseOpFunc
}

func NewFn(name string, min, max int, fn Evaluable, vp ValidateCb, gn FnGenNodeCb) *Fn {
	var f Fn
	f.SetName(name)
	f.SetMaxArgs(max)
	f.SetMinArgs(min)
	f.SetEvalFn(fn)
	f.SetIsValidParam(vp)
	f.SetNodeFn(gn)
	// TODO: give a context
	f.MakeArgTable(true)
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
func NewCastFn(castTp int, fn Evaluable, vp ValidateCb, gn FnGenNodeCb) *CastFn {
	var f CastFn
	// TODO: caseFnTp to readable name
	f.SetName(fmt.Sprintf("%d", castTp))
	f.SetCastFnTp(castTp)
	f.SetMaxArgs(1)
	f.SetMinArgs(1)
	f.SetEvalFn(fn)
	f.SetNodeFn(gn)

	// ? fix me: return type is related to castTp
	f.SetIsValidParam(vp)
	// TODO: give a context
	f.MakeArgTable(true)
	return &f
}
