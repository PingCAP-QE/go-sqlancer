package transformer

import "github.com/pingcap/parser/ast"

type (
	CombinedTransformer struct {
		t1 Transformer
		t2 Transformer
	}
)

func Combine(t1, t2 Transformer) *CombinedTransformer {
	return &CombinedTransformer{t1: t1, t2: t2}
}

func (h *CombinedTransformer) Transform(nodes []ast.ResultSetNode) []ast.ResultSetNode {
	return h.t2.Transform(h.t1.Transform(nodes))
}
