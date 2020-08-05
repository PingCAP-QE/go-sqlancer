package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/chaos-mesh/go-sqlancer/pkg/sqlancer"
	_ "github.com/go-sql-driver/mysql"
)

var (
	// TODO: clean these items
	conf      = sqlancer.NewConfig()
	dsn       = flag.String("dsn", "", "dsn of target db for testing")
	duration  = flag.Duration("duration", 5*time.Hour, "fuzz duration")
	silent    = flag.Bool("silent", false, "silent when verify failed")
	logLevel  = flag.String("log-level", "info", "set log level: info, warn, error, debug [default: info]")
	mode      = flag.String("approach", "pqs|norec|tlp", "use some of PQS, NoREC or TLP method, split by vertical bar")
	depth     = flag.Int("depth", 1, "sql depth")
	viewCount = flag.Int("view-count", 10, "count of views to be created")
	hint      = flag.Bool("enable-hint", false, "enable sql hint for TiDB")
	exprIdx   = flag.Bool("enable-expr-idx", false, "enable create expression index")
)

func main() {
	loadConfig()
	mutasql, err := sqlancer.NewMutaSql(conf)
	if err != nil {
		panic(fmt.Sprintf("new mutasql failed, error: %+v\n", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	mutasql.Start(ctx)
}

func loadConfig() {
	flag.Parse()
	if err := conf.SetDSN(*dsn); err != nil {
		panic(err)
	}
	conf.ViewCount = *viewCount
	conf.Depth = *depth
	conf.Silent = *silent
	conf.LogLevel = *logLevel
	conf.EnableHint = *hint
	conf.EnableExprIndex = *exprIdx
	// if len(*mode) == 0 {
	// 	panic("empty mode param set")
	// }
	// approaches := strings.Split(*mode, "|")
	// if len(approaches) == 0 {
	// 	panic("no testing approach is specified")
	// }
	// for _, approach := range approaches {
	// 	switch strings.ToLower(approach) {
	// 	case "pqs":
	// 		conf.EnablePQSApproach = true
	// 	case "norec":
	// 		conf.EnableNoRECApproach = true
	// 	case "tlp":
	// 		conf.EnableTLPApproach = true
	// 	default:
	// 		panic(fmt.Sprintf("unknown testing approach: %s", approach))
	// 	}
	// }
}
