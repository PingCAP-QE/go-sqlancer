package transformer

import (
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
	resultSetNodes := make([][]ast.ResultSetNode, 0)
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

// TODO: use error to tell connot deal the node with NoREC
func norec(node *ast.SelectStmt) ([]ast.ResultSetNode, error) {
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
	exprNode := &driver.ValueExpr{}
	tp := types.NewFieldType(mysql.TypeLonglong)
	tp.Flag = 128
	exprNode.TexprNode.SetType(tp)
	exprNode.Datum.SetInt64(1)
	countField := ast.SelectField{
		Expr: &ast.AggregateFuncExpr{
			F: "count",
			Args: []ast.ExprNode{
				exprNode,
			},
		},
	}
	p.Fields.Fields = []*ast.SelectField{&countField}
	results = append(results, &p)

	// use sum and subquery to avoid optimization
	sum := &ast.SelectField{
		Expr: &ast.AggregateFuncExpr{
			F: "sum",
			Args: []ast.ExprNode{
				&ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Name: model.NewCIStr(NOREC_TMP_COL_NAME),
					},
				},
			},
		},
	}
	// avoid empty result set such as `SELECT FROM`
	if q.Where == nil {
		q.Where = exprNode
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
