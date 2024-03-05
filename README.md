# Duckdb-adbc

### clone the repo from github
git clone https://github.com/dataphion/duckdb-adbc.git

### install all required packages
navigate inside duckdb-adbc
go get .
go mod tidy


### start the project 
go run main.go

duckdb-adbc will be exposed in <server_ip>:8899

access ducdb using any adbc driver 
Example using python adbc driver

import adbc_driver_flightsql.dbapi
conn = adbc_driver_flightsql.dbapi.connect("grpc://<server_ip>:8899")
try:
    print("Connected to server")
    cursor = conn.cursor()
    # Run all DuckDB based query using cursor.execute
    cursor.execute("show all tables")
    print(cursor.fetchall())
finally:
    # Close the cursor and connection when done
    cursor.close()
    conn.close()


