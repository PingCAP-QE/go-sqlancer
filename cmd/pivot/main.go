package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/chaos-mesh/private-wreck-it/pkg/pivot"
	_ "github.com/go-sql-driver/mysql"
)

const (
	nmDSN       = "dsn"
	nmViewCount = "view"
	nmDuration  = "duration"
	sqlDepth    = "depth"
	silentMode  = "silent"
)

var (
	conf      = pivot.NewConfig()
	dsn       = flag.String(nmDSN, "", "dsn of target db for testing")
	viewCount = flag.Int(nmViewCount, 10, "count of views to be created")
	duration  = flag.Duration(nmDuration, 5*time.Minute, "fuzz duration")
	depth     = flag.Int(sqlDepth, 1, "sql depth")
	silent    = flag.Bool(silentMode, false, "silent when verify failed")
)

func main() {
	loadConfig()
	p, err := pivot.NewPivot(conf)
	if err != nil {
		panic(fmt.Sprintf("new pivot failed, error: %+v\n", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	fmt.Printf("start pivot test\n")
	p.Start(ctx)
	fmt.Printf("wait for finish\n")

	<-ctx.Done()
	p.Close()
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
		conf.ViewCount = *viewCount
	}
	if actualFlags[sqlDepth] {
		conf.Depth = *depth
	}
	if actualFlags[silentMode] {
		conf.Silent = *silent
	}
}
