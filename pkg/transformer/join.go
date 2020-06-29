package transformer

import "github.com/pingcap/parser/ast"

type (
	JoinTransformer struct {
		t1 Transformer
		t2 Transformer
	}
)

func Join(t1, t2 Transformer) *JoinTransformer {
	return &JoinTransformer{t1: t1, t2: t2}
}

func (h *JoinTransformer) Transform(nodes [][]ast.ResultSetNode) [][]ast.ResultSetNode {
	return h.t2.Transform(h.t1.Transform(nodes))
}
