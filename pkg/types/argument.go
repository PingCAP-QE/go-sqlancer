package types

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/juju/errors"
)

// TODO: can it copy from tidb/types?
const (
	// Null included by any types
	// TypeNullArg               uint64 = 1 << iota
	TypeNonFormattedStringArg uint64 = 1 << iota
	TypeDatetimeLikeStringArg uint64 = 1 << iota // datetime formatted string: "1990-10-11"
	TypeNumberLikeStringArg   uint64 = 1 << iota // "1.001" " 42f_xa" start with number
	TypeIntArg                uint64 = 1 << iota
	TypeFloatArg              uint64 = 1 << iota
	TypeDatetimeArg           uint64 = 1 << iota
	TypeDatatimeLikeArg       uint64 = TypeDatetimeArg | TypeDatetimeLikeStringArg
	TypeStringArg             uint64 = TypeNonFormattedStringArg | TypeDatatimeLikeArg | TypeNumberLikeStringArg
	TypeNumberArg             uint64 = TypeIntArg | TypeFloatArg
	TypeNumberLikeArg         uint64 = TypeNumberArg | TypeNumberLikeStringArg
	// Array, Enum, Blob to be completed

	AnyArg uint64 = math.MaxUint64
)

var (
	SupportArgs []uint64 = []uint64{
		TypeFloatArg,
		TypeIntArg,
		TypeNonFormattedStringArg,
		TypeDatetimeLikeStringArg,
		TypeNumberLikeStringArg,
	}
)

type ArgTable interface {
	// reserve for variable arg length op/fn
	RandByFilter([]*uint64, *uint64) ([]uint64, error)
	// for table is [1, 3, 4] => 3, [1, 2, 5] => 3, [1, 2, 2] => 5
	// input [1, 2, nil], 3 => select where [1, 2, *] => 3
	// in previous example, is [1, 2, 5] => 3
	// returns [[1, 2, 5, 3]]
	Filter([]*uint64, *uint64) ([][]uint64, error)
	Insert(uint64, ...uint64)
}
type OpFuncArg0DTable uint64 // only the return type
type OpFuncArgNDTable struct {
	table [][]uint64
	n     int
}

// TODO: implement me: for infinite args
type OpFuncArgInfTable struct{}

func (t *OpFuncArg0DTable) Filter([]*uint64, *uint64) ([][]uint64, error) {
	return [][]uint64{{(uint64)(*t)}}, nil
}

func (t *OpFuncArg0DTable) RandByFilter([]*uint64, *uint64) ([]uint64, error) {
	return []uint64{(uint64)(*t)}, nil
}
func (t *OpFuncArg0DTable) Insert(ret uint64, _ ...uint64) {
	*t = (OpFuncArg0DTable)(ret)
}

func (t *OpFuncArgNDTable) Filter(args []*uint64, ret *uint64) ([][]uint64, error) {
	realArgs := make([]*uint64, len(args))
	copy(realArgs, args)
	if len(realArgs) < t.n {
		for i := len(realArgs); i < t.n; i++ {
			realArgs = append(realArgs, nil)
		}
	}
	if len(realArgs) > t.n {
		realArgs = realArgs[:t.n]
	}
	result := make([][]uint64, 0)
	for _, i := range t.table {
		for idx, j := range realArgs {
			if j == nil {
				continue
			}
			if *j != i[idx] {
				goto NEXT_LOOP
			}
		}
		if ret == nil || *ret == i[len(i)-1] {
			result = append(result, i[:])
		}
	NEXT_LOOP:
	}
	if len(result) == 0 {
		return nil, errors.New(fmt.Sprintf("empty set after filter, args: %+v, ret: %v", args, ret))
	}
	return result, nil
}

func (t *OpFuncArgNDTable) RandByFilter(args []*uint64, ret *uint64) ([]uint64, error) {
	total, err := t.Filter(args, ret)
	if err != nil {
		return nil, err
	}
	return total[rand.Intn(len(total))], nil
}

func (t *OpFuncArgNDTable) Insert(ret uint64, args ...uint64) {
	// here do NOT check duplicate records
	if len(args) != t.n {
		panic(fmt.Sprintf("arguments number doesnot match n: %d, args: %d", t.n, len(args)))
	}
	args = append(args, ret)
	t.table = append(t.table, args)
}

func NewArgTable(dimension int) ArgTable {
	switch dimension {
	case 0:
		return new(OpFuncArg0DTable)
	case 1, 2, 3:
		return &OpFuncArgNDTable{n: dimension, table: make([][]uint64, 0)}
	default:
		panic(fmt.Sprintf("more args (%d) are not supported present", dimension))
	}
}
