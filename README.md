
## Duckdb-adbc

This project focuses on enabling remote query execution on a DuckDB database using the Asynchronous Database Connector (ADBC). DuckDB is an embedded analytical database known for its high performance, and ADBC is a library designed to provide an asynchronous interface for database connectivity.

## Why ADBC driver is useful for DuckDB

While most of the time data analysis on local device is made possible by DuckDB, there are scenarios where it becomes necessary to execute queries on remote server which has better hardware capabilities compared to user laptops. This will help in avoiding SSH to remote server and executing the SQL queries. Additionally data needs to be migrated from host server to local device for further analysis. Query execution on remote devices directly using ADBC driver helps solve these problem. Not to mention BI tools have started supporting ADBC driver for query execution.

Feel free to reach out to adithya.bhagavath@dataphion.com for any queries. Raise PR for any feature enhancements.

## Upcoming features

* Add Data Governance and Security Features for DuckDB using Data Guardiium(Will be available for public release by 15th Mar 2024).
* Distributed query execution using DuckDB (TBD).

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

```run
go run main.go
```

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