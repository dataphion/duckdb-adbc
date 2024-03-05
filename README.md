# Project Name

Duckdb-adbc

## Description

This project focuses on enabling remote query execution on a DuckDB database using the Asynchronous Database Connector (ADBC). DuckDB is an embedded analytical database known for its high performance, and ADBC is a library designed to provide an asynchronous interface for database connectivity.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)

## Installation

```cmd
git clone https://github.com/dataphion/duckdb-adbc.git

cd duckdb-adbc

go get .

go mod tidy
```

## Usage

go run main.go

duckdb-adbc will be exposed in <server_ip>:8899

Example using python adbc driver

```python
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
```