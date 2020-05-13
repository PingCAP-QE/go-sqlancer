package knownbugs

import (
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
)

var (
	issue16788 KnownBug = func(d *Dustbin) bool {

		var travel func(ast.Node, bool) bool
		travel = func(n ast.Node, found bool) bool {
			if found {
				return true
			}
			switch tp := n.(type) {
			case *ast.ParenthesesExpr:
				return travel(tp.Expr, false)
			case *ast.BinaryOperationExpr:
				if tp.Op != opcode.EQ {
					return travel(tp.L, false) || travel(tp.R, false)
				}
				if lType, ok := tp.L.(ast.ValueExpr); ok {
					if lType.GetType().Tp != mysql.TypeFloat && lType.GetType().Tp != mysql.TypeDouble {
						return false
					}
					if rType, ok := tp.R.(*ast.ColumnNameExpr); !ok {
						return false
					} else {
						rTp := rType.GetType().Tp
						if rTp != mysql.TypeInt24 &&
							rTp != mysql.TypeLong &&
							rTp != mysql.TypeLonglong &&
							rTp != mysql.TypeShort &&
							rTp != mysql.TypeTiny {
							return false
						}
						// TODO: select from pivot row and verify it is NULL

						return true
					}
				}
				if rType, ok := tp.R.(ast.ValueExpr); ok {
					if rType.GetType().Tp != mysql.TypeFloat && rType.GetType().Tp != mysql.TypeDouble {
						return false
					}
					if lType, ok := tp.L.(*ast.ColumnNameExpr); !ok {
						return false
					} else {
						lTp := lType.GetType().Tp
						if lTp != mysql.TypeInt24 &&
							lTp != mysql.TypeLong &&
							lTp != mysql.TypeLonglong &&
							lTp != mysql.TypeShort &&
							lTp != mysql.TypeTiny {
							return false
						}
						// TODO: select from pivot row and verify it is NULL

						return true
					}
				}
				return false
			case *ast.UnaryOperationExpr:
				return travel(tp.V, false)
			case *ast.IsNullExpr:
				return travel(tp.Expr, false)
			case *ast.ColumnNameExpr:
				return false
			case ast.ValueExpr:
				return false
			default:
				panic(fmt.Sprintf("unreachable, tp: %+v", tp))
			}
		}

		var mergeOnClause func(ast.Node) []ast.ExprNode
		mergeOnClause = func(n ast.Node) []ast.ExprNode {
			result := make([]ast.ExprNode, 0)
			switch t := n.(type) {
			case *ast.Join:
				result = append(result, t.On.Expr)
				result = append(result, mergeOnClause(t.Left)...)
				result = append(result, mergeOnClause(t.Right)...)
			}

			return result
		}

		fmt.Println("LENGTH: ", len(d.Stmts))
		// get last one stmt
		stmt := d.Stmts[:len(d.Stmts)][0]
		switch t := stmt.(type) {
		case *ast.SelectStmt:
			exprs := mergeOnClause(t.From.TableRefs)
			exprs = append(exprs, t.Where)
			for _, e := range exprs {
				if travel(e, false) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
)
