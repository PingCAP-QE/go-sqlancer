package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/chaos-mesh/go-sqlancer/pkg/sqlancer"
	_ "github.com/go-sql-driver/mysql"
)

const (
	nmDSN        = "dsn"
	nmViewCount  = "view"
	nmDuration   = "duration"
	nmSqlDepth   = "depth"
	nmSilentMode = "silent"
	nmLogLevel   = "log-level"
	nmHint       = "hint"
	nmExprIdx    = "expr-index"
	nmMode       = "mode"
)

var (
	conf      = sqlancer.NewConfig()
	dsn       = flag.String(nmDSN, "", "dsn of target db for testing")
	viewCount = flag.Int(nmViewCount, 10, "count of views to be created")
	duration  = flag.Duration(nmDuration, 5*time.Hour, "fuzz duration")
	depth     = flag.Int(nmSqlDepth, 1, "sql depth")
	silent    = flag.Bool(nmSilentMode, false, "silent when verify failed")
	logLevel  = flag.String(nmLogLevel, "info", "set log level: info, warn, error, debug [default: info]")
	hint      = flag.Bool(nmHint, false, "enable sql hint for TiDB")
	exprIdx   = flag.Bool(nmExprIdx, false, "enable create expression index")
	mode      = flag.String(nmMode, "pqs|norec", "use NoRec or PQS method or both, split by vertical bar")
)

func main() {
	loadConfig()
	sqlancer, err := sqlancer.NewSQLancer(conf)
	if err != nil {
		panic(fmt.Sprintf("new sqlancer failed, error: %+v\n", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	sqlancer.Start(ctx)
}

func loadConfig() {
	flag.Parse()

	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	if actualFlags[nmDSN] {
		if err := conf.SetDSN(*dsn); err != nil {
			panic(err)
		}
	} else {
		panic("empty dsn")
	}
	if actualFlags[nmViewCount] {
		conf.TotalViewCount = *viewCount
	}
	if actualFlags[nmSqlDepth] {
		conf.Depth = *depth
	}
	if actualFlags[nmSilentMode] {
		conf.Silent = *silent
	}
	if actualFlags[nmLogLevel] {
		conf.LogLevel = *logLevel
	}
	if actualFlags[nmHint] {
		conf.EnableHint = *hint
	}
	if actualFlags[nmExprIdx] {
		conf.EnableExprIndex = *exprIdx
	}
	if actualFlags[nmMode] {
		if len(*mode) == 0 {
			panic("empty mode param set")
		}
		conf.EnableNoRECMode, conf.EnablePQSMode = false, false
		approaches := strings.Split(*mode, "|")
		hasSet := false
		for _, i := range approaches {
			if strings.ToLower(i) == "norec" {
				conf.EnableNoRECMode = true
				hasSet = true
			}
			if strings.ToLower(i) == "pqs" {
				conf.EnablePQSMode = true
				hasSet = true
			}
		}
		if !hasSet {
			panic("no valid mode param set")
		}
	}
}
