package operator

import (
	"github.com/chaos-mesh/go-sqlancer/pkg/types"
)

var (
	UnaryOps    = make(types.OpFuncIndex)
	BinaryOps   = make(types.OpFuncIndex)
	MultiaryOps = make(types.OpFuncIndex)
)
