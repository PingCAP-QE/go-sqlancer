package main

import (
	"flag"
	"fmt"
	"github.com/pingcap/log"
	"github.com/chaos-mesh/private-wreck-it/pkg/executor"
	"github.com/chaos-mesh/private-wreck-it/pkg/generator"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

var (
	dsn = flag.String("dsn", "", "config file path")
)

func main() {
	if err := doGenerate(); err != nil {
		panic(err)
	}
}

func doGenerate() error {
	flag.Parse()

	//client, err := sql.Open("mysql", *dsn)
	//if err != nil {
	//	log.Fatal(err)
	//	return
	//}
	//
	//tableNum := rand.Intn(5) + 1

	dbDsn := "root@tcp(127.0.0.1:4000)/"
	e, err := executor.New(dbDsn, "test")
	if err != nil {
		return err
	}

	e.Exec("drop database if exists test")
	e.Exec("create database test")
	e.Exec("use test")

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < r.Intn(10)+1; i++ {
		sql, _ := e.GenerateDDLCreateTable()
		err := e.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("create table failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
			return err
		}
	}

	err = e.ReloadSchema()
	if err != nil {
		log.Error("reload data failed!")
		return err
	}
	ddlOpt := &generator.DDLOptions{
		OnlineDDL: true,
		Tables:    []string{},
	}
	for i := 0; i < r.Intn(10); i++ {
		sql, _ := e.GenerateDDLCreateIndex(ddlOpt)
		fmt.Println(sql)
		err = e.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("create index failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
			return err
		}
	}

	for i := 0; i < r.Intn(30)+2; i++ {
		sql, _ := e.GenerateDMLInsert()
		err = e.Exec(sql.SQLStmt)
		if err != nil {
			log.L().Error("insert data failed", zap.String("sql", sql.SQLStmt), zap.Error(err))
			return err
		}
	}
	return nil
}
