package util

import (
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Min get min fron two int
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MinInt ...
func MinInt(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// MaxInt ...
func MaxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// Rd same to rand.Intn
func Rd(n int) int {
	return rand.Intn(n)
}

// RdRange rand int in range
func RdRange(n, m int) int {
	if n == m {
		return n
	}
	if m < n {
		n, m = m, n
	}
	return n + rand.Intn(m-n)
}

// RdFloat64 rand float64
func RdFloat64() float64 {
	return rand.Float64()
}

// RdDate rand date
func RdDate() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 1, 0, time.UTC).Unix()
	max := time.Date(2100, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdTimestamp return same format as RdDate except rand range
// TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07'
func RdTimestamp() time.Time {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2038, 1, 19, 3, 14, 7, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

// RdString rand string with given length
func RdString(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(33, 127)
		// char '\' and '"' should be escaped
		if charCode == 92 || charCode == 34 {
			charCode++
			// res = fmt.Sprintf("%s%s", res, "\\")
		}
		res = fmt.Sprintf("%s%s", res, string(rune(charCode)))
	}
	return res
}

// RdStringChar rand string with given length, letter chars only
func RdStringChar(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		charCode := RdRange(97, 123)
		res = fmt.Sprintf("%s%s", res, string(rune(charCode)))
	}
	return res
}
