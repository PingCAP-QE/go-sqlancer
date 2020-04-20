// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package data_gen

import (
	"fmt"
	//"github.com/pingcap/parser/ast"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/types"
	"github.com/chaos-mesh/private-wreck-it/pkg/go-sqlsmith/util"
	"strings"
	"time"

	"github.com/pingcap/parser/mysql"
	tidbTypes "github.com/pingcap/tidb/types"
	uuid "github.com/satori/go.uuid"
)

// GetUUID return uuid
func GetUUID() string {
	return strings.ToUpper(uuid.NewV4().String())
}

// GenerateRandDataItem rand data item with rand type
func GenerateRandDataItem() interface{} {
	var dataType string
	switch util.Rd(6) {
	case 0:
		dataType = "varchar"
	case 1:
		dataType = "text"
	case 2:
		dataType = "int"
	case 3:
		dataType = "float"
	case 4:
		dataType = "timestamp"
	case 5:
		dataType = "datetime"
	}
	column := &types.Column{DataType: dataType}
	if dataType == "varchar" {
		column.DataLen = 100
	}
	return GenerateDataItem(column)
}

// GenerateDataItemString rand data with given type
func GenerateDataItemString(column *types.Column) string {
	d := GenerateDataItem(column)
	switch c := d.(type) {
	case string:
		return c
	case int:
		return fmt.Sprintf("\"%d\"", c)
	case time.Time:
		return c.Format("2006-01-02 15:04:05")
	case tidbTypes.Time:
		return c.String()
	case float64:
		return fmt.Sprintf("%f", c)
	}
	return "not implement data transfer"
}

// GenerateDataItem rand data interface with given type
func GenerateDataItem(column *types.Column) interface{} {
	var res interface{}
	// there will be 1/3 possibility return nil
	//if !column.HasOption(ast.ColumnOptionNotNull) && util.RdRange(0, 3) == 0 {
	//	return nil
	//}
	switch column.DataType {
	case "varchar":
		res = GenerateStringItemLen(column.DataLen)
	case "text":
		res = GenerateStringItem()
	case "int":
		res = GenerateIntItem()
	case "datetime":
		res = GenerateTiDBDateItem()
	case "timestamp":
		res = GenerateTiDBTimestampItem()
	case "float":
		res = GenerateFloatItem()
	}
	return res
}

// GenerateStringItem generate string item
func GenerateStringItem() string {
	return strings.ToUpper(GenerateStringItemLen(100))
}

func GenerateStringItemLen(length int) string {
	return util.RdStringChar(util.Rd(length))
}

// GenerateIntItem generate int item
func GenerateIntItem() int {
	return util.Rd(2147483647)
}

// GenerateFloatItem generate float item
func GenerateFloatItem() float64 {
	return float64(util.Rd(100000)) * util.RdFloat64()
}

// GenerateDateItem generate date item
func GenerateDateItem() time.Time {
	t := util.RdDate()
	for ifDaylightTime(t) {
		t = util.RdDate()
	}
	return t
}

// GenerateTimestampItem generate timestamp item
func GenerateTimestampItem() time.Time {
	t := util.RdTimestamp()
	for ifDaylightTime(t) {
		t = util.RdTimestamp()
	}
	return t
}

// GenerateTiDBDateItem generate date item
func GenerateTiDBDateItem() tidbTypes.Time {
	// return tidbTypes.Time{
	// 	Time: tidbTypes.FromGoTime(GenerateDateItem()),
	// 	Type: mysql.TypeDatetime,
	// }
	return tidbTypes.NewTime(tidbTypes.FromGoTime(GenerateDateItem()), mysql.TypeDatetime, 0)
}

func GenerateTiDBTimestampItem() tidbTypes.Time {
	return tidbTypes.NewTime(tidbTypes.FromGoTime(GenerateTimestampItem()), mysql.TypeDatetime, 0)
}

func ifDaylightTime(t time.Time) bool {
	if t.Year() < 1986 || t.Year() > 1991 {
		return false
	}
	if t.Month() < 4 || t.Month() > 9 {
		return false
	}
	return true
}
