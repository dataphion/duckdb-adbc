package main

import (
	"adbc_server/db_driver/duckdb_server"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql"
)

func main() {
	var (
		host = flag.String("host", "0.0.0.0", "hostname to bind to")
		port = flag.Int("port", 8899, "port to bind to")
	)

	flag.Parse()
	db, err := duckdb_server.CreateDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fmt.Println("Created Instance of Duck DB...")

	srv, err := duckdb_server.NewDuckDBFlightSQLServer(db)
	if err != nil {
		log.Fatal(err)
	}

	server := flight.NewServerWithMiddleware(nil)
	s := flightsql.NewFlightServer(srv)
	server.RegisterFlightService(s)
	server.Init(net.JoinHostPort(*host, strconv.Itoa(*port)))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)

	fmt.Println("Starting SQLite Flight SQL Server on", server.Addr(), "...")

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
