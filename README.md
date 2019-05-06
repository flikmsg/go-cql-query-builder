# go-cql-query-builder
📇 a super tiny query builder for cassandra, made in go  

## To do
- [X] Add UPDATE support
- [X] Add DELETE support

## Usage
```go
package main

import (
	"log"

	cqlquery "github.com/flikmsg/go-cql-query-builder"
)

func main() {
	if _, err := cqlquery.Connect([]string{"127.0.0.1"}, "test_keyspace"); err != nil {
		log.Fatal(err)
	}

	query := cqlquery.Select("users_by_username", []string{"user_id", "username"}, map[string]interface{}{
		"username": "tyler",
	})

	results := query.Iter()
	defer results.Close()

	m := map[string]interface{}{}
	if results.MapScan(m) {
		log.Printf("User ID: %s, Username: %s", m["user_id"], m["username"])
	} else {
		log.Println("No user found.")
	}
}
```