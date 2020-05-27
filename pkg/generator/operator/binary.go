package operator

import (
	"fmt"
	"math/rand"

	"go.uber.org/zap"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
)

var (
	BinaryOps = make(types.OpFuncMap)

	// comparisionValidator stands for a comparision function that results a int value
	comparisionValidator = func(args ...uint64) (uint64, bool, error) {
		if len(args) != 2 {
			panic("require two params")
		}
		a, b := args[0], args[1]
		resultType := types.TypeIntArg | types.TypeFloatArg
		// because binary ops almost reflexive, we can just swap a and b then judge again
		for i := 0; i < 2; i++ {
			switch a {
			case types.TypeIntArg, types.TypeFloatArg:
				if b&^types.TypeNonFormattedStringArg == 0 {
					return resultType, true, nil
				}
				if b&^(types.TypeNumberLikeArg|types.TypeDatetimeLikeStringArg) == 0 {
					return resultType, false, nil
				}
			case types.TypeNonFormattedStringArg:
				if b&^types.TypeStringArg == 0 {
					return resultType, false, nil
				}
				if b&^types.TypeDatetimeArg == 0 {
					// return ERROR 1525
					return 0, false, errors.New("invalid type")
				}
				if b&^types.TypeNumberLikeStringArg == 0 {
					return resultType, false, nil
				}
			case types.TypeNumberLikeStringArg:
				if b&^(types.TypeNumberLikeStringArg|types.TypeDatetimeLikeStringArg) == 0 {
					return resultType, false, nil
				}
				if b&^types.TypeDatetimeArg == 0 {
					return 0, false, errors.New("invalid type")
				}
			case types.TypeDatetimeArg:
				if b&^types.TypeDatetimeLikeArg == 0 {
					return resultType, false, nil
				}
			case types.TypeDatetimeLikeStringArg:
				if b&^types.TypeDatetimeLikeStringArg == 0 {
					return resultType, false, nil
				}
			}
			a, b = b, a
		}
		log.L().Error("a and b are unexpected type", zap.Uint64("a", a), zap.Uint64("b", b))
		panic("unreachable")
	}

	defaultBinaryOpGenNode = func(opCode opcode.Op) types.TypedExprNodeGenSel {
		return func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
			// generate op node and call cb to generate its child node
			valLeft := parser_driver.ValueExpr{}
			valRight := parser_driver.ValueExpr{}
			op, ok := this.(*types.BaseOpFunc)
			if !ok {
				panic("should can transfer to BaseOpFunc")
			}

			argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			if len(argList) == 0 {
				return nil, parser_driver.ValueExpr{},
					errors.New(fmt.Sprintf("cannot find valid param for type(%d) returned", ret))
			}
			args := argList[rand.Intn(len(argList))]
			firstArg := args[0]
			secondArg := args[1]

			node := ast.BinaryOperationExpr{}
			node.Op = opCode
			node.L, valLeft, err = cb(firstArg)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			node.R, valRight, err = cb(secondArg)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			evalResult, err := op.Eval(valLeft, valRight)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			return &node, evalResult, nil
		}
	}

	Gt = types.NewOp(opcode.GT, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.Compare(a, b) > 0)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.GT))

	Lt = types.NewOp(opcode.LT, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.Compare(a, b) < 0)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.LT))

	Ne = types.NewOp(opcode.NE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.Compare(a, b) != 0)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.NE))

	Eq = types.NewOp(opcode.EQ, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.Compare(a, b) == 0)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.EQ))

	Ge = types.NewOp(opcode.GE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.Compare(a, b) >= 0)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.GE))

	Le = types.NewOp(opcode.LE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.Compare(a, b) <= 0)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.LE))

	LogicXor = types.NewOp(opcode.LogicXor, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.ConvertToBoolOrNull(a) != util.ConvertToBoolOrNull(b))
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.LogicXor))

	LogicAnd = types.NewOp(opcode.LogicAnd, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		boolA := util.ConvertToBoolOrNull(a)
		boolB := util.ConvertToBoolOrNull(b)
		if boolA*boolB == 0 {
			e.SetValue(false)
			return e, nil
		}
		if boolA == -1 || boolB == -1 {
			e.SetValue(nil)
			return e, nil
		}
		e.SetValue(true)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.LogicAnd))

	LogicOr = types.NewOp(opcode.LogicOr, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		boolA := util.ConvertToBoolOrNull(a)
		boolB := util.ConvertToBoolOrNull(b)
		if boolA == 1 || boolB == 1 {
			e.SetValue(true)
			return e, nil
		}
		if boolA == -1 || boolB == -1 {
			e.SetValue(nil)
			return e, nil
		}
		e.SetValue(false)
		return e, nil
	}, comparisionValidator, defaultBinaryOpGenNode(opcode.LogicOr))
)

func init() {
	// DONOT op on non-date format types
	for _, f := range []*types.Op{Lt, Gt, Le, Ge, Ne, Eq, LogicXor, LogicAnd, LogicOr} {
		BinaryOps.Add(f)
		util.RegisterToOpFnIndex(f)
	}
}
