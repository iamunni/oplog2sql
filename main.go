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
	Object2   map[string]interface{} `json:"o2"`
}

func GenerateSql(oplog string) (string, error) {
	var opLogEntry OpLogEntry
	if err := json.Unmarshal([]byte(oplog), &opLogEntry); err != nil {
		return "", err
	}

	switch opLogEntry.Operation {
	case "i":
		return generateInsertSql(opLogEntry)
	case "u":
		return generateupdateSql(opLogEntry)
	}

	return "", nil
}

func generateInsertSql(opLogEntry OpLogEntry) (string, error) {
	sql := fmt.Sprintf("INSERT INTO %s", opLogEntry.Namespace)
	columnNames := make([]string, 0, len(opLogEntry.Object))
	for columnName := range opLogEntry.Object {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)

	columnValues := make([]string, 0, len(opLogEntry.Object))
	for _, columnName := range columnNames {
		columnValues = append(columnValues, columnValueToString(opLogEntry.Object[columnName]))
	}
	sql = fmt.Sprintf("%s (%s) VALUES (%s);", sql, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "))
	return sql, nil
}

func generateupdateSql(opLogEntry OpLogEntry) (string, error) {
	sql := fmt.Sprintf("UPDATE %s SET", opLogEntry.Namespace)
	diffMap, ok := opLogEntry.Object["diff"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("Invalid Update Oplog")
	}

	if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
		columnValues := make([]string, 0, len(setMap))
		for columnName, columnValue := range setMap {
			columnValues = append(columnValues, fmt.Sprintf("%s = %s", columnName, columnValueToString(columnValue)))
		}
		sort.Strings(columnValues)
		sql = fmt.Sprintf("%s %s", sql, strings.Join(columnValues, ", "))
	} else if unsetMap, ok := diffMap["d"].(map[string]interface{}); ok {
		columnValues := make([]string, 0, len(unsetMap))
		for columnName := range unsetMap {
			columnValues = append(columnValues, fmt.Sprintf("%s = NULL", columnName))
		}
		sort.Strings(columnValues)
		sql = fmt.Sprintf("%s %s", sql, strings.Join(columnValues, ", "))
	} else {
		return "", fmt.Errorf("Invalid Update Oplog")
	}

	whereColumnValues := make([]string, 0, len(opLogEntry.Object2))
	for columnName, columnValue := range opLogEntry.Object2 {
		whereColumnValues = append(whereColumnValues, fmt.Sprintf("%s = %s", columnName, columnValueToString(columnValue)))
	}

	sql = fmt.Sprintf("%s WHERE %s;", sql, strings.Join(whereColumnValues, " AND "))
	return sql, nil
}

func columnValueToString(columnValue interface{}) string {
	switch columnValue.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", columnValue)
	case bool:
		return fmt.Sprintf("%t", columnValue)
	default:
		return fmt.Sprintf("'%v'", columnValue)
	}
}
