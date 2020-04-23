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
)

var (
	conf      = pivot.NewConfig()
	dsn       = flag.String(nmDSN, "", "dsn of target db for testing")
	viewCount = flag.Int(nmViewCount, 10, "count of views to be created")
	duration  = flag.Duration(nmDuration, 5*time.Minute, "fuzz duration")
)

func main() {
	flag.Parse()
	loadConfig()

	if *dsn == "" {
		panic("no dsn in arguments")
	}

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

	flag.Parse()
}
