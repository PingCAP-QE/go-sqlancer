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

package types

import (
	"regexp"
	"strconv"
)

var (
	typePattern = regexp.MustCompile(`([a-z]*)\(?([0-9]*)\)?`)
)

// Column defines database column
type Column struct {
	Table  CIStr
	Name   CIStr
	Type   string
	Length int
	Null   bool
}

// Clone makes a replica of column
func (c *Column) Clone() Column {
	return Column{
		Table:  c.Table,
		Name:   c.Name,
		Type:   c.Type,
		Length: c.Length,
		Null:   c.Null,
	}
}

// ParseType parse types and data length
func (c *Column) ParseType(t string) {
	matches := typePattern.FindStringSubmatch(t)
	if len(matches) != 3 {
		return
	}
	c.Type = matches[1]
	if matches[2] != "" {
		l, err := strconv.Atoi(matches[2])
		if err == nil {
			c.Length = l
		}
	} else {
		c.Length = 0
	}
}

// AddOption add option for column
// func (c *Column) AddOption(opt ast.ColumnOptionType) {
// 	for _, option := range c.Options {
// 		if option == opt {
// 			return
// 		}
// 	}
// 	c.Options = append(c.Options, opt)
// }

// // HasOption return is has the given option
// func (c *Column) HasOption(opt ast.ColumnOptionType) bool {
// 	for _, option := range c.Options {
// 		if option == opt {
// 			return true
// 		}
// 	}
// 	return false
// }
