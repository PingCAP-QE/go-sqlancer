package types

import (
	"strings"

	"github.com/pingcap/parser/model"
)

type CIStr string

// String ...
func (c CIStr) String() string {
	return string(c)
}

// ToModel converts to CIStr in model
func (c CIStr) ToModel() model.CIStr {
	return model.NewCIStr(string(c))
}

// EqString reports whether CIStr is equal to a string
func (c CIStr) EqString(s string) bool {
	return strings.EqualFold(string(c), s)
}

// Eq reports whether CIStr is equal to another CIStr
func (c CIStr) Eq(s CIStr) bool {
	return strings.EqualFold(string(c), string(s))
}

// HasPrefix implement strings.HasPrefix
func (c CIStr) HasPrefix(s string) bool {
	return strings.HasPrefix(strings.ToLower(string(c)), strings.ToLower(strings))
}
