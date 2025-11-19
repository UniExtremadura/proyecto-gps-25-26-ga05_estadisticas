package db

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

var Session *gocql.Session

func InitDatabase() {
	cluster := gocql.NewCluster("estadisticas-db")
	cluster.Keyspace = "estadisticas_keyspace"
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "user",
		Password: "12345",
	}
	cluster.ConnectTimeout = 10 * time.Second

	var err error
	Session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal("Error conectando a Cassandra: ", err)
	}

	fmt.Println("Conectado a Cassandra exitosamente")
}

func CloseDatabase() {
	if Session != nil {
		Session.Close()
		fmt.Println("Sesión Cassandra cerrada")
	}
}
