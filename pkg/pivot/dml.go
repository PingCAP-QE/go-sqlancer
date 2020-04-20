package pivot

import (
	"github.com/pingcap/parser/ast"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

type ExprNode struct {
	ast.ExprNode
}

// TODO: can it copy from tidb/types?
const (
	StringArg           = 1 << iota
	DatetimeAsStringArg = 1 << iota // datetime formatted string: "1990-10-11"
	IntArg              = 1 << iota
	FloatArg            = 1 << iota
	DatetimeArg         = 1 << iota
	NullArg             = 1 << iota

	AnyArg = 0xFFFF
)

type Function struct {
	// [0] indicates first param; if args are infinite, last item will be duplicated
	// [0] is [StringArg] => [AnyArg^DatetimeArg]: first arg is string then datetime is invalid as second arg
	AcceptType []map[int]int
	// min and max; -1 indicates infinite
	MinArgs int
	MaxArgs int
	Name    string
	Eval    func(a, b parser_driver.ValueExpr) (parser_driver.ValueExpr, error)
}

func (f *Function) NewAcceptTypeMap() *map[int]int {
	m := make(map[int]int)
	m[StringArg] = AnyArg
	m[DatetimeAsStringArg] = AnyArg
	m[IntArg] = AnyArg
	m[FloatArg] = AnyArg
	m[DatetimeArg] = AnyArg
	m[NullArg] = AnyArg
	return &m
}
