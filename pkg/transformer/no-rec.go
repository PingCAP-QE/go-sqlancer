package transformer

import (
	"github.com/juju/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"
)

const (
	NOREC_TMP_TABLE_NAME = "tmp_table"
	NOREC_TMP_COL_NAME   = "tmp_col"
)

/** transformer examples:
 * select A from B join C on D where E
 *     => select count(*) from B join C on D where E
 *     => select sum(t_0.c_0) from (select (E is true) as c_0 from B join C on D) as t_0
 */
var NoREC TransformerSingleton = func(nodeSet [][]ast.ResultSetNode) [][]ast.ResultSetNode {
	resultSetNodes := make([][]ast.ResultSetNode, len(nodeSet))
	copy(resultSetNodes, nodeSet)
	for _, nodes := range nodeSet {
		nodeArr := make([]ast.ResultSetNode, 0)
		for _, node := range nodes {
			switch t := node.(type) {
			case *ast.UnionStmt:
			case *ast.SelectStmt:
				if eqNodes, err := norec(t); err == nil {
					nodeArr = append(nodeArr, eqNodes...)
				} else {
					log.L().Info("norec trans error", zap.Error(err))
				}
			default:
				panic("type not implemented")
			}
		}
		if len(nodeArr) > 0 {
			resultSetNodes = append(resultSetNodes, nodeArr)
		} else {
			log.L().Warn("empty nodeArr")
		}
	}
	return resultSetNodes
}

type NoRECVisitor struct {
	hasAggFn bool
}

func (v *NoRECVisitor) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	if _, ok := n.(*ast.AggregateFuncExpr); ok {
		v.hasAggFn = true
	}
	return n, v.hasAggFn
}
func (v *NoRECVisitor) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, v.hasAggFn
}

func norec(node *ast.SelectStmt) ([]ast.ResultSetNode, error) {
	if node.Fields != nil {

		v := &NoRECVisitor{}
		node.Fields.Accept(v)
		if v.hasAggFn {
			return nil, errors.New("not support aggregation fn in result field")
		}
	}
	results := make([]ast.ResultSetNode, 0)
	// Is there a clone method available on ast.node?
	p := *node
	p.Fields = &ast.FieldList{
		Fields: nil,
	}
	p.TableHints = nil
	q := *node
	q.Fields = &ast.FieldList{
		Fields: nil,
	}
	q.TableHints = nil
	// drop all result fields and put count(*) into Fields
	countField := ast.SelectField{
		Expr: &ast.AggregateFuncExpr{
			F: "count",
			Args: []ast.ExprNode{
				makeIntConstValue(1),
			},
		},
	}
	p.Fields.Fields = []*ast.SelectField{&countField}
	results = append(results, &p)

	// use sum and subquery to avoid optimization
	sum := &ast.SelectField{
		Expr: &ast.FuncCallExpr{
			FnName: model.NewCIStr("IFNULL"),
			Args: []ast.ExprNode{
				&ast.AggregateFuncExpr{
					F: "sum",
					Args: []ast.ExprNode{
						&ast.ColumnNameExpr{
							Name: &ast.ColumnName{
								Name: model.NewCIStr(NOREC_TMP_COL_NAME),
							},
						},
					},
				},
				makeIntConstValue(0),
			},
		},
	}
	// avoid empty result set such as `SELECT FROM`
	if q.Where == nil {
		q.Where = makeIntConstValue(1)
	} else {
		// switch t := q.Where.(type) {
		// case *ast.IsNullExpr:
		// 	goto SKIP_RECTIFY
		// case *ast.UnaryOperationExpr:
		// 	if t.Op == opcode.Not {
		// 		goto SKIP_RECTIFY
		// 	}
		// }
		if _, ok := q.Where.(*ast.IsNullExpr); !ok {
			// make it return 1 as true through adding `IS TRUE`
			q.Where = &ast.IsTruthExpr{
				Expr: q.Where,
				True: 1,
			}
		}
		// SKIP_RECTIFY:
	}
	q.Fields.Fields = []*ast.SelectField{
		{
			Expr:   q.Where,
			AsName: model.NewCIStr(NOREC_TMP_COL_NAME),
		},
	}
	q.Where = nil
	// clear sql hint
	q.TableHints = make([]*ast.TableOptimizerHint, 0)
	wrapped := &ast.SelectStmt{
		SelectStmtOpts: &ast.SelectStmtOpts{
			SQLCache: true,
		},
		Fields: &ast.FieldList{
			Fields: []*ast.SelectField{
				sum,
			},
		},
		From: &ast.TableRefsClause{
			TableRefs: &ast.Join{
				Left: &ast.TableSource{
					AsName: model.NewCIStr(NOREC_TMP_TABLE_NAME),
					Source: &q,
				},
			},
		},
	}
	results = append(results, wrapped)
	return results, nil
}

func makeIntConstValue(i int64) *driver.ValueExpr {
	exprNode := &driver.ValueExpr{}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flag = 128
	exprNode.TexprNode.SetType(tp)
	exprNode.Datum.SetInt64(i)

	return exprNode
}
