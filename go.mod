module github.com/chaos-mesh/private-wreck-it

go 1.13

require (
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/go-sql-driver/mysql v1.5.0
	github.com/juju/errors v0.0.0-20190930114154-d42613fe1ab9
	github.com/juju/loggo v0.0.0-20190526231331-6e530bcce5d8 // indirect
	github.com/juju/testing v0.0.0-20191001232224-ce9dec17d28b // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200317021010-cd90cc2a7d87
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.5.0 // indirect
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.14.0
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
)

replace github.com/pingcap/tidb => github.com/pingcap/tidb v0.0.0-20200317142013-5268094afe05
