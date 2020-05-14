package knownbugs

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
)

var (
	issue16788 KnownBug = func(d *Dustbin) bool {

		v := NewVisitor()
		mergeOnClause := func(n ast.Node) []ast.ExprNode {
			result := make([]ast.ExprNode, 0)
			v.SetEnter(func(in ast.Node) (ast.Node, bool) {
				switch t := in.(type) {
				case *ast.Join:
					if t.On != nil {
						result = append(result, t.On.Expr)
					}
				}
				return in, false
			})
			defer func() {
				v.ClearEnter()
			}()
			n.Accept(&v)
			return result
		}

		find := func(n ast.Node) bool {
			result := false
			v.SetEnter(func(in ast.Node) (ast.Node, bool) {
				switch tp := in.(type) {
				case *ast.ColumnNameExpr:
					return in, true
				case ast.ValueExpr:
					return in, true
				case *ast.BinaryOperationExpr:
					if tp.Op != opcode.EQ {
						return in, false
					}
					if lType, ok := tp.L.(ast.ValueExpr); ok {
						if lType.GetType().Tp != mysql.TypeFloat && lType.GetType().Tp != mysql.TypeDouble && lType.GetType().Tp != mysql.TypeNewDecimal {
							return in, false
						}
						if rType, ok := tp.R.(*ast.ColumnNameExpr); !ok {
							return in, false
						} else {
							rTp := rType.GetType().Tp
							if rTp != mysql.TypeInt24 &&
								rTp != mysql.TypeLong &&
								rTp != mysql.TypeLonglong &&
								rTp != mysql.TypeShort &&
								rTp != mysql.TypeTiny {
								return in, false
							}
							// TODO: select from pivot row and verify it is NULL

							result = true
							return in, true
						}
					}
					if rType, ok := tp.R.(ast.ValueExpr); ok {
						if rType.GetType().Tp != mysql.TypeFloat && rType.GetType().Tp != mysql.TypeDouble && rType.GetType().Tp != mysql.TypeNewDecimal {
							return in, false
						}
						if lType, ok := tp.L.(*ast.ColumnNameExpr); !ok {
							return in, false
						} else {
							lTp := lType.GetType().Tp
							if lTp != mysql.TypeInt24 &&
								lTp != mysql.TypeLong &&
								lTp != mysql.TypeLonglong &&
								lTp != mysql.TypeShort &&
								lTp != mysql.TypeTiny {
								return in, false
							}
							// TODO: select from pivot row and verify it is NULL

							result = true
							return in, true
						}
					}
					return in, false
				default:
					return in, false
				}
			})
			defer func() {
				v.ClearEnter()
			}()
			n.Accept(&v)
			return result
		}

		stmt := d.Stmts[:len(d.Stmts)][0]
		switch t := stmt.(type) {
		case *ast.SelectStmt:
			exprs := mergeOnClause(t.From.TableRefs)
			exprs = append(exprs, t.Where)
			for _, e := range exprs {
				if find(e) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
)
