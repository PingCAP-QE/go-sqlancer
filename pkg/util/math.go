package util

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
