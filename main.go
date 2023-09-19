package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func main() {
	fmt.Println("Hello world")
}

type OpLogEntry struct {
	Operation string                 `json:"op"`
	Namespace string                 `json:"ns"`
	Object    map[string]interface{} `json:"o"`
}

func GenerateInsertSql(oplog string) (string, error) {
	var opLogEntry OpLogEntry
	if err := json.Unmarshal([]byte(oplog), &opLogEntry); err != nil {
		return "", err
	}

	switch opLogEntry.Operation {
	case "i":
		sql := fmt.Sprintf("INSERT INTO %s", opLogEntry.Namespace)
		columnNames := make([]string, 0, len(opLogEntry.Object))
		for columnName := range opLogEntry.Object {
			columnNames = append(columnNames, columnName)
		}
		sort.Strings(columnNames)

		columnValues := make([]string, 0, len(opLogEntry.Object))
		for _, columnName := range columnNames {
			columnValues = append(columnValues, getColumnValue(opLogEntry.Object[columnName]))
		}
		sql = fmt.Sprintf("%s (%s) VALUES (%s);", sql, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "))
		return sql, nil
	}

	return "", nil
}

func getColumnValue(columnValue interface{}) string {
	switch columnValue.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", columnValue)
	case bool:
		return fmt.Sprintf("%t", columnValue)
	default:
		return fmt.Sprintf("'%v'", columnValue)
	}
}
