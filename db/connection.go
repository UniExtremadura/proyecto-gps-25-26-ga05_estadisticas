package db

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

var Session *gocql.Session

func InitDatabase() {
	// Primero conectar sin keyspace específico para crearlo si no existe
	cluster := gocql.NewCluster("estadisticas-db")
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "user",
		Password: "12345",
	}
	cluster.ConnectTimeout = 10 * time.Second
	cluster.Timeout = 30 * time.Second

	// Crear sesión temporal sin keyspace
	tempSession, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Error conectando a Cassandra: ", err)
	}
	defer tempSession.Close()

	fmt.Println("Conectado a Cassandra, creando keyspace y tablas...")

	// Crear keyspace si no existe
	createKeyspaceQuery := `
		CREATE KEYSPACE IF NOT EXISTS estadisticas_keyspace
		WITH replication = {
			'class': 'SimpleStrategy',
			'replication_factor': 1
		}`

	if err := tempSession.Query(createKeyspaceQuery).Exec(); err != nil {
		log.Fatal("Error creando keyspace: ", err)
	}

	// Ahora conectar con el keyspace
	cluster.Keyspace = "estadisticas_keyspace"
	Session, err = cluster.CreateSession()
	if err != nil {
		log.Fatal("Error conectando al keyspace: ", err)
	}

	// Crear tablas
	createTables()

	// Insertar datos de ejemplo
	insertSampleData()

	fmt.Println("Conectado a Cassandra exitosamente y base de datos inicializada")
}

func createTables() {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS escucha (
			idUsuario int,
			idCancion int,
			fecha timestamp,
			PRIMARY KEY ((idUsuario), fecha)
		)`,

		`CREATE TABLE IF NOT EXISTS compraAlbum (
			idUsuario int,
			idAlbum int,
			fecha timestamp,
			PRIMARY KEY ((idUsuario), fecha)
		)`,

		`CREATE TABLE IF NOT EXISTS compraMerch (
			idUsuario int,
			idMerch int,
			fecha timestamp,
			cantidad int,
			PRIMARY KEY ((idUsuario), fecha)
		)`,
	}

	for _, query := range queries {
		if err := Session.Query(query).Exec(); err != nil {
			log.Printf("Error creando tabla: %v", err)
		}
	}
}

func insertSampleData() {
	// Verificar si ya existen datos para no duplicar
	var count int
	if err := Session.Query("SELECT COUNT(*) FROM escucha").Scan(&count); err != nil {
		log.Printf("Error verificando datos existentes: %v", err)
		return
	}

	// Si ya hay datos, no insertar
	if count > 0 {
		fmt.Println("Datos ya existen, omitiendo inserción")
		return
	}

	// Función helper para parsear fechas
	parseTime := func(timeStr string) time.Time {
		t, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			log.Printf("Error parseando fecha %s: %v", timeStr, err)
			return time.Now()
		}
		return t
	}

	// Insertar datos de escucha
	escuchas := []struct {
		idUsuario int
		idCancion int
		fecha     time.Time
	}{
		{1, 1, parseTime("2025-01-10T10:00:00Z")},
		{2, 1, parseTime("2025-02-10T11:00:00Z")},
		{3, 1, parseTime("2025-03-10T10:00:00Z")},
		{3, 1, parseTime("2025-01-10T12:00:00Z")},
		{4, 1, parseTime("2025-02-10T10:50:00Z")},
		{1, 2, parseTime("2025-02-12T11:30:00Z")},
		{1, 2, parseTime("2025-03-12T11:30:00Z")},
		{1, 2, parseTime("2025-05-12T11:20:00Z")},
		{2, 3, parseTime("2025-03-15T09:20:00Z")},
		{3, 3, parseTime("2025-03-15T09:11:20Z")},
		{3, 4, parseTime("2025-04-18T14:45:00Z")},
		{4, 5, parseTime("2025-05-22T16:10:00Z")},
	}

	for _, e := range escuchas {
		query := `INSERT INTO escucha (idUsuario, idCancion, fecha) VALUES (?, ?, ?)`
		if err := Session.Query(query, e.idUsuario, e.idCancion, e.fecha).Exec(); err != nil {
			log.Printf("Error insertando escucha: %v", err)
		}
	}

	// Insertar compras de álbumes
	comprasAlbum := []struct {
		idUsuario int
		idAlbum   int
		fecha     time.Time
	}{
		{1, 1, parseTime("2025-01-11T12:00:00Z")},
		{1, 2, parseTime("2025-02-11T12:00:00Z")},
	}

	for _, c := range comprasAlbum {
		query := `INSERT INTO compraAlbum (idUsuario, idAlbum, fecha) VALUES (?, ?, ?)`
		if err := Session.Query(query, c.idUsuario, c.idAlbum, c.fecha).Exec(); err != nil {
			log.Printf("Error insertando compraAlbum: %v", err)
		}
	}

	// Insertar compras de merchandising
	comprasMerch := []struct {
		idUsuario int
		idMerch   int
		fecha     time.Time
		cantidad  int
	}{
		{1, 1, parseTime("2025-01-12T09:10:00Z"), 1},
		{1, 2, parseTime("2025-01-13T09:11:00Z"), 2},
	}

	for _, c := range comprasMerch {
		query := `INSERT INTO compraMerch (idUsuario, idMerch, fecha, cantidad) VALUES (?, ?, ?, ?)`
		if err := Session.Query(query, c.idUsuario, c.idMerch, c.fecha, c.cantidad).Exec(); err != nil {
			log.Printf("Error insertando compraMerch: %v", err)
		}
	}

	fmt.Println("Datos de ejemplo insertados correctamente")
}

func CloseDatabase() {
	if Session != nil {
		Session.Close()
		fmt.Println("Sesión Cassandra cerrada")
	}
}
