package generator

import (
	"fmt"
	"math/rand"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	. "github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	parser_types "github.com/pingcap/parser/types"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"
)

func (g *Generator) constValueExpr(tp uint64) (ast.ValueExpr, parser_driver.ValueExpr) {
	val := parser_driver.ValueExpr{}
	// return null randomly
	if Rd(6) == 0 {
		val.SetNull()
		return ast.NewValueExpr(nil, "", ""), val
	}

	// choose one type from tp
	possibleTypes := make([]uint64, 0)
	for tp != 0 {
		possibleTypes = append(possibleTypes, tp&^(tp-1))
		tp &= tp - 1
	}
	tp = possibleTypes[rand.Intn(len(possibleTypes))]

	switch tp {
	case types.TypeFloatArg:
		switch Rd(6) {
		case 0, 1:
			val.SetFloat64(0)
			return ast.NewValueExpr(float64(0), "", ""), val
		case 2:
			val.SetFloat64(1.0)
			return ast.NewValueExpr(float64(1.0), "", ""), val
		case 3:
			val.SetFloat64(-1.0)
			return ast.NewValueExpr(float64(-1.0), "", ""), val
		default:
			f := RdFloat64()
			val.SetFloat64(f)
			return ast.NewValueExpr(f, "", ""), val
		}
	case types.TypeIntArg:
		switch Rd(6) {
		case 0, 1:
			val.SetInt64(0)
			return ast.NewValueExpr(0, "", ""), val
		case 2:
			val.SetInt64(1)
			return ast.NewValueExpr(1, "", ""), val
		case 3:
			val.SetInt64(-1)
			return ast.NewValueExpr(-1, "", ""), val
		default:
			i := RdInt64()
			val.SetInt64(i)
			return ast.NewValueExpr(i, "", ""), val
		}
	case types.TypeNonFormattedStringArg:
		switch Rd(3) {
		case 0, 1:
			s := RdString(Rd(10))
			val.SetString(s, "")
			return ast.NewValueExpr(s, "", ""), val
		default:
			val.SetString("", "")
			return ast.NewValueExpr("", "", ""), val
		}
	case types.TypeNumberLikeStringArg:
		var s string = fmt.Sprintf("%f", RdFloat64())
		if RdBool() {
			s = fmt.Sprintf("%d", RdInt64())
		}
		val.SetString(s, "")
		return ast.NewValueExpr(s, "", ""), val
	case types.TypeDatetimeArg:
		if RdBool() {
			// is TIMESTAMP
			t := tidb_types.NewTime(tidb_types.FromGoTime(RdTimestamp()), mysql.TypeTimestamp, int8(Rd(7)))
			val.SetMysqlTime(t)
			return ast.NewValueExpr(t, "", ""), val
		}
		// is DATETIME
		t := tidb_types.NewTime(tidb_types.FromGoTime(RdDate()), mysql.TypeDatetime, 0)
		val.SetMysqlTime(t)
		return ast.NewValueExpr(t, "", ""), val
	case types.TypeDatetimeLikeStringArg:
		s := RdTimestamp().Format("2006-01-02 15:04:05")
		val.SetString(s, "")
		return ast.NewValueExpr(s, "", ""), val
	default:
		log.L().Error("this type is not implemented", zap.Uint64("tp", tp))
		panic("unreachable")
	}
}

func (g *Generator) columnExpr(genCtx *GenCtx, arg uint64) (*ast.ColumnNameExpr, parser_driver.ValueExpr, error) {
	val := parser_driver.ValueExpr{}
	tables := &genCtx.ResultTables
	if genCtx.IsInUpdateDeleteStmt || genCtx.IsInExprIndex {
		tables = &genCtx.UsedTables
	}
	randTable := (*tables)[Rd(len(*tables))]
	tempCols := make([]types.Column, 0)
	for i := range randTable.Columns {
		if TransStringType(randTable.Columns[i].Type)&arg != 0 {
			tempCols = append(tempCols, randTable.Columns[i])
		}
	}
	if len(tempCols) == 0 {
		return nil, val, errors.New(fmt.Sprintf("no valid column as arg %d table %s", arg, randTable.Name))
	}
	randColumn := tempCols[Rd(len(tempCols))]
	colName, typeStr := randColumn.Name, randColumn.Type
	col := new(ast.ColumnNameExpr)
	col.Name = &ast.ColumnName{
		Table: randTable.GetAliasName().ToModel(),
		Name:  colName.ToModel(),
	}
	col.Type = parser_types.FieldType{}
	col.SetType(tidb_types.NewFieldType(TransToMysqlType(TransStringType(typeStr))))

	// no pivot rows
	// so value is generate by random
	if genCtx.IsInUpdateDeleteStmt {
		tp := TransStringType(typeStr)
		_, val = g.constValueExpr(tp)
		return col, val, nil
	}

	// find the column value in pivot rows
	for key, value := range genCtx.unwrapPivotRows {
		originTableName := col.Name.Table.L
		for k, v := range genCtx.TableAlias {
			if v == originTableName {
				originTableName = k
				break
			}
		}
		originColumnName := col.Name.Name.L
		if key == fmt.Sprintf("%s.%s", originTableName, originColumnName) {
			val.SetValue(value)
			if tmpTable, ok := genCtx.TableAlias[col.Name.Table.L]; ok {
				col.Name.Table = model.NewCIStr(tmpTable)
			}
			break
		}
	}

	return col, val, nil
}

func (g *Generator) generateExpr(ctx *GenCtx, valueTp uint64, depth int) (ast.ExprNode, parser_driver.ValueExpr, error) {
	if valueTp == 0 {
		log.L().Warn("required return type is empty")
		e := parser_driver.ValueExpr{}
		e.SetValue(nil)
		return ast.NewValueExpr(nil, "", ""), e, nil
	}
	// select one type from valueTp randomly
	// TODO: make different types have different chance
	// e.g. INT/FLOAT appears more often than NumberLikeString...
	possibleTypes := make([]uint64, 0)
	for valueTp != 0 {
		possibleTypes = append(possibleTypes, valueTp&^(valueTp-1))
		valueTp &= valueTp - 1
	}
	tp := possibleTypes[rand.Intn(len(possibleTypes))]

	// TODO: fix me!
	// when tp is NumberLikeString, no function can return it
	// but in ALMOST cases, it can be replaced by INT/FLOAT
	if tp == types.TypeNumberLikeStringArg {
		if RdBool() {
			tp = types.TypeFloatArg
		}
		tp = types.TypeIntArg
	}

	// generate leaf node
	if depth == 0 {
		if Rd(3) > 1 {
			exprNode, value, err := g.columnExpr(ctx, tp)
			if err == nil {
				return exprNode, value, nil
			}
		}
		exprNode, value := g.constValueExpr(tp)
		return exprNode, value, nil
	}

	// select a function with return type tp
	fn, err := OpFuncGroupByRet.RandOpFn(tp)
	if err != nil {
		log.L().Warn("generate fn or op failed", zap.Error(err))
		// if no op/fn satisfied requirement
		// generate const instead
		exprNode, value := g.constValueExpr(tp)
		return exprNode, value, nil
	}

	pthese := ast.ParenthesesExpr{}
	var value parser_driver.ValueExpr
	pthese.Expr, value, err = fn.Node(func(childTp uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		return g.generateExpr(ctx, childTp, depth-1)
	}, tp)
	if err != nil {
		return nil, parser_driver.ValueExpr{}, errors.Trace(err)
	}
	return &pthese, value, nil
}
