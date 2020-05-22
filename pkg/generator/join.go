package generator

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

func (g *Generator) genJoin(node *ast.Join, genCtx *GenCtx) {
	if node.Right == nil {
		if node, ok := node.Left.(*ast.TableSource); ok {
			if tn, ok := node.Source.(*ast.TableName); ok {
				if table := genCtx.findUsedTableByName(tn.Name.L); table != nil {
					genCtx.ResultTables = append(genCtx.ResultTables, *table)
					return
				}
			}
		}
		panic("unreachable")
	}

	if right, ok := node.Right.(*ast.TableSource); ok {
		var (
			leftTables []types.Table
			rightTable types.Table
		)
		switch node := node.Left.(type) {
		case *ast.Join:
			g.genJoin(node, genCtx)
			leftTables = genCtx.ResultTables
		case *ast.TableSource:
			{
				if tn, ok := node.Source.(*ast.TableName); ok {
					if table := genCtx.findUsedTableByName(tn.Name.L); table != nil {
						tmpTable := genCtx.createTmpTable()
						node.AsName = model.NewCIStr(tmpTable)
						leftTables = []types.Table{table.Rename(tmpTable)}
						genCtx.TableAlias[table.Name.String()] = tmpTable
						break
					}
				}
			}
		default:
			panic("unreachable")
		}
		if table := genCtx.findUsedTableByName(right.Source.(*ast.TableName).Name.L); table != nil {
			tmpTable := genCtx.createTmpTable()
			right.AsName = model.NewCIStr(tmpTable)
			rightTable = table.Rename(tmpTable)
			genCtx.TableAlias[table.Name.String()] = tmpTable
		} else {
			panic("unreachable")
		}
		allTables := append(leftTables, rightTable)
		// usedTables := genCtx.UsedTables
		genCtx.ResultTables = allTables
		// genCtx.UsedTables = allTables
		// defer func() {
		// 	genCtx.UsedTables = usedTables
		// }()
		node.On = &ast.OnCondition{}
		// for _, table := range genCtx.ResultTables {
		// 	fmt.Println(table.Name, table.AliasName)
		// }
		node.On.Expr = g.WhereClauseAst(genCtx, 1)
		return
	}

	panic("unreachable")
}

// func (g *Generator) RectifyJoin(node *ast.Join, genCtx *GenCtx) {
// 	if node.Right == nil {
// 		if node, ok := node.Left.(*ast.TableSource); ok {
// 			if tn, ok := node.Source.(*ast.TableName); ok {
// 				for _, table := range genCtx.UsedTables {
// 					if table.Name.EqModel(tn.Name) {
// 						return
// 					}
// 				}
// 			}
// 		}
// 		panic("unreachable")
// 	}

// 	if _, ok := node.Right.(*ast.TableSource); ok {
// 		node.On.Expr = g.RectifyCondition(node.On.Expr, genCtx)
// 		if node, ok := node.Left.(*ast.Join); ok {
// 			g.RectifyJoin(node, genCtx)
// 		}
// 		return
// 	}

// 	panic("unreachable")
// }
