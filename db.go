package cqlquery

import (
	"github.com/gocql/gocql"
)

// Conn is the global database connection.
var Conn *gocql.Session

// Connect is a function that attempts to connect to a Cassandra cluster.
func Connect(hosts []string, keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	var err error
	// Set the global variable.
	Conn, err = cluster.CreateSession()

	return Conn, err
}
