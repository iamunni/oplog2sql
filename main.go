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

func GenerateSql(oplog string) ([]string, error) {
	sqls := []string{}
	var opLogEntries []OpLogEntry
	if err := json.Unmarshal([]byte(oplog), &opLogEntries); err != nil {
		var opLogEntry OpLogEntry
		if err := json.Unmarshal([]byte(oplog), &opLogEntry); err != nil {
			return sqls, err
		}
		opLogEntries = append(opLogEntries, opLogEntry)
	}

	opLogMap := make(map[string]bool)

	for _, opLogEntry := range opLogEntries {
		sql, err := generteSql(opLogEntry, opLogMap)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql...)
	}

	return sqls, nil
}

func generteSql(opLogEntry OpLogEntry, opLogMap map[string]bool) ([]string, error) {
	sqls := []string{}

	switch opLogEntry.Operation {
	case "i":
		// Create Schema
		schema := strings.Split(opLogEntry.Namespace, ".")[0]
		if exists := opLogMap[schema]; !exists {
			sqls = append(sqls, generateCreateSchemaSql(schema))
			opLogMap[schema] = true
		}

		//Create table
		if exists := opLogMap[opLogEntry.Namespace]; !exists {
			sqls = append(sqls, generateCreateTableSql(opLogEntry, opLogMap))
			opLogMap[opLogEntry.Namespace] = true
		} else if isEligibleForAlterTable(opLogEntry, opLogMap) {
			sqls = append(sqls, generateAlterTableSql(opLogEntry, opLogMap))
		}

		sql, err := generateInsertSql(opLogEntry)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	case "u":
		sql, err := generateUpdateSql(opLogEntry)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	case "d":
		sql, err := generateDeleteSql(opLogEntry)
		if err != nil {
			return sqls, err
		}
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func generateCreateSchemaSql(schema string) string {
	return fmt.Sprintf("CREATE SCHEMA %s;", schema)
}

func generateCreateTableSql(opLogEntry OpLogEntry, opLogMap map[string]bool) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (", opLogEntry.Namespace))
	columnNames := getColumnNames(opLogEntry.Object)
	seperator := ""
	for _, columnName := range columnNames {
		columnValue := opLogEntry.Object[columnName]
		columnDataType := getColumnSqlDataType(columnName, columnValue)
		cacheKey := fmt.Sprintf("%s.%s", opLogEntry.Namespace, columnName)
		opLogMap[cacheKey] = true
		sb.WriteString(fmt.Sprintf("%s%s %s", seperator, columnName, columnDataType))
		seperator = ", "
	}
	sb.WriteString(");")
	return sb.String()
}

func isEligibleForAlterTable(opLogEntry OpLogEntry, opLogMap map[string]bool) bool {
	columnNames := getColumnNames(opLogEntry.Object)
	for _, columnName := range columnNames {
		cacheKey := fmt.Sprintf("%s.%s", opLogEntry.Namespace, columnName)
		if !opLogMap[cacheKey] {
			return true
		}
	}
	return false
}

func generateAlterTableSql(opLogEntry OpLogEntry, opLogMap map[string]bool) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("ALTER TABLE %s", opLogEntry.Namespace))
	seperator := " "
	for columnName := range opLogEntry.Object {
		columnValue := opLogEntry.Object[columnName]
		columnDataType := getColumnSqlDataType(columnName, columnValue)
		cacheKey := fmt.Sprintf("%s.%s", opLogEntry.Namespace, columnName)
		if !opLogMap[cacheKey] {
			sb.WriteString(fmt.Sprintf("%sADD COLUMN %s %s", seperator, columnName, columnDataType))
			seperator = ", "
		}
	}
	sb.WriteString(";")
	return sb.String()
}

func getColumnSqlDataType(columnName string, columnValue interface{}) string {
	colDataType := ""
	switch columnValue.(type) {
	case int, int8, int16, int32, int64:
		colDataType = "INTEGER"
	case float32, float64:
		colDataType = "FLOAT"
	case bool:
		colDataType = "BOOLEAN"
	default:
		colDataType = "VARCHAR(255)"
	}

	if columnName == "_id" {
		colDataType += " PRIMARY KEY"
	}
	return colDataType
}

func generateInsertSql(opLogEntry OpLogEntry) (string, error) {
	sql := fmt.Sprintf("INSERT INTO %s", opLogEntry.Namespace)
	columnNames := getColumnNames(opLogEntry.Object)

	columnValues := make([]string, 0, len(opLogEntry.Object))
	for _, columnName := range columnNames {
		columnValues = append(columnValues, columnValueToString(opLogEntry.Object[columnName]))
	}
	sql = fmt.Sprintf("%s (%s) VALUES (%s);", sql, strings.Join(columnNames, ", "), strings.Join(columnValues, ", "))
	return sql, nil
}

func getColumnNames(data map[string]interface{}) []string {
	columnNames := make([]string, 0, len(data))
	for columnName := range data {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}

func generateUpdateSql(opLogEntry OpLogEntry) (string, error) {
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

func generateDeleteSql(opLogEntry OpLogEntry) (string, error) {
	sql := fmt.Sprintf("DELETE FROM %s", opLogEntry.Namespace)
	whereColumnValues := make([]string, 0, len(opLogEntry.Object))
	for columnName, columnValue := range opLogEntry.Object {
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
