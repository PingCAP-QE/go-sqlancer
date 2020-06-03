package generator

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

func (g *Generator) walkTableRefs(node *ast.Join, genCtx *GenCtx) {
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
			g.walkTableRefs(node, genCtx)
			leftTables = genCtx.ResultTables
		case *ast.TableSource:
			if tn, ok := node.Source.(*ast.TableName); ok {
				if table := genCtx.findUsedTableByName(tn.Name.L); table != nil {
					tmpTable := genCtx.createTmpTable()
					node.AsName = model.NewCIStr(tmpTable)
					leftTables = []types.Table{table.Rename(tmpTable)}
					genCtx.TableAlias[table.Name.String()] = tmpTable
					break
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
		node.On.Expr = g.ConditionClause(genCtx, 1)
		return
	}

	panic("unreachable")
}
