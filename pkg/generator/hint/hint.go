package hint

import (
	"fmt"
	"math/rand"

	"github.com/chaos-mesh/go-sqlancer/pkg/types"
	"github.com/chaos-mesh/go-sqlancer/pkg/util"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

type hintClass struct {
	name     string
	minArg   int
	maxArg   int
	constArg bool
	mysql    bool
	stable   bool
}

var hintKeywords = []*hintClass{
	// with no args
	{"hash_agg", 0, 0, false, false, false},
	{"stream_agg", 0, 0, false, false, false},
	{"agg_to_cop", 0, 0, false, false, false},
	{"read_consistent_replica", 0, 0, false, false, false},
	{"no_index_merge", 0, 0, false, false, false},

	// with bool (TRUE or FALSE)
	{"use_toja", 1, 1, false, false, false},
	{"enable_plan_cache", 1, 1, false, false, false},
	{"use_cascades", 1, 1, false, false, false},

	// these have been renamed
	// {"tidb_hj", 2, 3, false, false, true},
	// {"tidb_smj", 2, 3, false, false, true},
	// {"tidb_inlj", 2, 3, false, false, true},
	// with 2 or more args
	{"hash_join", 1, -1, false, true, false},
	{"merge_join", 1, -1, false, false, false},
	{"inl_join", 1, -1, false, false, false},

	// with int (byte)
	{"memory_quota", 1, 1, false, false, false},
	// with int (ms)
	{"max_execution_time", 1, 1, false, false, false},
}

var indexHintKeywords = []*hintClass{
	// with table name and at least one idx name
	{"use_index", 2, -1, false, false, false},
	{"ignore_index", 2, -1, false, false, false},
	{"use_index_merge", 2, -1, false, false, false},
}

// these will not be generated for some reason
var disabledHintKeywords = []*hintClass{
	{"qb_name", 0, 0, false, false, false},

	// not released?
	{"time_range", 2, -1, false, false, false},
	// storage type with tablename: TIKV[t1]
	{"read_from_storage", 2, -1, false, false, false},
	// not released?
	{"query_type", 1, 1, false, false, false},

	{"inl_hash_join", 1, -1, false, false, false},
	{"inl_merge_join", 1, -1, false, false, false},
}

func GenerateHintExpr(usedTables []types.Table) (h *ast.TableOptimizerHint) {
	enabledKeywords := hintKeywords
	mergedIndexes := make([]string, 0)
	for _, t := range usedTables {
		for _, i := range t.Indexes {
			mergedIndexes = append(mergedIndexes, t.Name.String()+"."+i.String())
		}
	}
	if len(mergedIndexes) > 0 {
		enabledKeywords = append(enabledKeywords, indexHintKeywords...)
	}
	h = new(ast.TableOptimizerHint)
	hintKeyword := enabledKeywords[util.Rd(len(enabledKeywords))]
	h.HintName = model.NewCIStr(hintKeyword.name)

	if hintKeyword.maxArg == 0 {
		return
	}

	if hintKeyword.maxArg == 1 {
		switch hintKeyword.name {
		case "use_toja", "enable_plan_cache", "use_cascades":
			h.HintData = util.RdBool()
		case "memory_quota":
			h.HintData = int64(util.RdRange(30720000, 40960000))
		case "max_execution_time":
			h.HintData = uint64(util.RdRange(500, 1500))
		default:
			panic(fmt.Sprintf("unreachable hintKeyword.name:%s", hintKeyword.name))
		}
		return
	}

	shuffledTables := make([]ast.HintTable, 0)
	for _, t := range usedTables {
		shuffledTables = append(shuffledTables, ast.HintTable{
			TableName: model.NewCIStr(t.Name.String()),
		})
	}
	rand.Shuffle(len(shuffledTables), func(i, j int) {
		shuffledTables[i], shuffledTables[j] = shuffledTables[j], shuffledTables[i]
	})

	shuffledIndexes := make([]model.CIStr, 0)
	for _, idx := range mergedIndexes {
		if idx != "" {
			shuffledIndexes = append(shuffledIndexes, model.NewCIStr(idx))
		}
	}
	rand.Shuffle(len(shuffledIndexes), func(i, j int) {
		shuffledIndexes[i], shuffledIndexes[j] = shuffledIndexes[j], shuffledIndexes[i]
	})

	switch hintKeyword.name {
	case "hash_join", "merge_join", "inl_join", "inl_hash_join", "inl_merge_join":
		if len(shuffledTables) < 2 {
			h = nil
			return
		}

		n := util.MinInt(util.Rd(4)+2, len(shuffledTables)) // avoid case n < 2
		for ; n > 0; n-- {
			h.Tables = append(h.Tables, shuffledTables[n-1])
		}
	case "use_index", "ignore_index", "use_index_merge":
		// if no table nor index return empty
		if len(shuffledTables) == 0 || len(shuffledIndexes) == 0 {
			h = nil
			return
		}
		h.Tables = append(h.Tables, shuffledTables[util.Rd(len(shuffledTables))])
		n := util.MinInt(util.Rd(4)+1, len(shuffledIndexes)) // avoid case n == 0
		for ; n > 0; n-- {
			h.Indexes = append(h.Indexes, shuffledIndexes[n-1])
		}
	default:
		panic(fmt.Sprintf("unreachable hintKeyword.name:%s", hintKeyword.name))
	}
	return
}
