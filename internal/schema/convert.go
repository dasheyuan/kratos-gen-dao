package schema

import "strings"

func convert2PackageName(tableName string) string {
	return strings.ToLower(strings.Replace(tableName, cUnderScore, "", -1))
}

//下划线->驼峰
func convertUnderScoreToCamel(name string) string {
	arr := strings.Split(name, cUnderScore)
	for i := 0; i < len(arr); i++ {
		arr[i] = lintName(strings.Title(arr[i]))
	}
	return strings.Join(arr, "")
}
