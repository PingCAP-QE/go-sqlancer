package util

// ComposeAllColumnTypes get all column type combinations
// length stands for the max types combined in one table, -1 to unlimit
func ComposeAllColumnTypes(length int, allColumnTypes []string) [][]string {
	var res [][]string
	if length == -1 {
		length = len(allColumnTypes)
	}
	for i := 1; i <= length; i++ {
		indexes := make([]int, i, i)
		for j := 0; j < i; j++ {
			indexes[j] = j
		}

		for {
			table := make([]string, i, i)
			for j, index := range indexes {
				table[j] = allColumnTypes[index]
			}
			res = append(res, table)

			finish := true
			for j := len(indexes) - 1; j >= 0; j-- {
				if indexes[j] < len(allColumnTypes)-(len(indexes)-j) {
					indexes[j]++
					for k := j + 1; k < len(indexes); k++ {
						indexes[k] = indexes[k-1] + 1
					}
					finish = false
					break
				}
			}
			if finish {
				break
			}
		}
	}
	return res
}
