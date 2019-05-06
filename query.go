package cqlquery

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

// Select creates a select statement.
func Select(table string, fields []string, query map[string]interface{}) *gocql.Query {
	var builder strings.Builder

	builder.WriteString("SELECT ")
	if fields == nil {
		builder.WriteString("* ")
	} else {
		builder.WriteString(strings.Join(fields, ",") + " ")
	}

	builder.WriteString("FROM " + table + " WHERE ")

	var queryFields []string
	var values []interface{}
	for k, v := range query {
		queryFields = append(queryFields, fmt.Sprintf("%s=?", k))
		values = append(values, v)
	}
	builder.WriteString(strings.Join(queryFields, " AND "))

	return Conn.Query(builder.String(), values...)
}

// Insert creates an insert statement.
func Insert(table string, query map[string]interface{}) *gocql.Query {
	var builder strings.Builder

	builder.WriteString("INSERT INTO ")
	builder.WriteString(table)
	builder.WriteString(" (")

	var queryFields []string
	var values []interface{}
	var valuePlaceholders []string
	for k, v := range query {
		queryFields = append(queryFields, k)
		values = append(values, v)
		valuePlaceholders = append(valuePlaceholders, "?")
	}
	builder.WriteString(strings.Join(queryFields, ","))
	builder.WriteString(")  VALUES (")
	builder.WriteString(strings.Join(valuePlaceholders, ","))
	builder.WriteString(")")

	return Conn.Query(builder.String(), values...)
}

// Update creates an update statement.
func Update(table string, query map[string]interface{}, changes map[string]interface{}) *gocql.Query {
	var builder strings.Builder

	builder.WriteString("UPDATE ")
	builder.WriteString(table)
	builder.WriteString(" SET ")

	var changeFields []string
	var values []interface{}
	for k, v := range changes {
		changeFields = append(changeFields, fmt.Sprintf("%s=?", k))
		values = append(values, v)
	}
	builder.WriteString(strings.Join(changeFields, ","))
	builder.WriteString(" WHERE ")
	var queryFields []string
	for k, v := range query {
		changeFields = append(queryFields, fmt.Sprintf("%s=?", k))
		values = append(values, v)
	}
	builder.WriteString(strings.Join(queryFields, ","))

	return Conn.Query(builder.String(), values...)
}

// Delete creates an insert statement.
func Delete(table string, query map[string]interface{}) *gocql.Query {
	var builder strings.Builder

	builder.WriteString("DELETE FROM ")
	builder.WriteString(table)
	builder.WriteString(" WHERE ")

	var fields []string
	var values []interface{}
	for k, v := range query {
		fields = append(fields, fmt.Sprintf("%s=?", k))
		values = append(values, v)
	}
	builder.WriteString(strings.Join(fields, " AND "))

	return Conn.Query(builder.String(), values...)
}
