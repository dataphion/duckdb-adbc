package duckdb_server

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/arrow/scalar"
	_ "github.com/marcboeker/go-duckdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func genRandomString() []byte {
	fmt.Println("DBG:: Inside getRandomString")
	const length = 16
	max := int('z')
	// don't include ':' as a valid byte to generate
	// because we use it as a separator for the transactions
	min := int('<')

	out := make([]byte, length)
	for i := range out {
		out[i] = byte(rand.Intn(max-min+1) + min)
	}
	return out
}

func StartServer() {
	print("Starting Server")
}

func CreateDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "./database.db")
	if err != nil {
		return nil, err
	}

	db.Exec("INSTALL aws")
	db.Exec("LOAD aws")
	db.Exec("INSTALL httpfs")
	db.Exec("LOAD httpfs")
	db.Exec("SET autoload_known_extensions=1")
	db.Exec("SET autoinstall_known_extensions=1")

	fmt.Println("In-memory DB created")
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func encodeTransactionQuery(query string, transactionID flightsql.Transaction) ([]byte, error) {
	fmt.Println("DBG:: COND 102")
	return flightsql.CreateStatementQueryTicket(
		bytes.Join([][]byte{transactionID, []byte(query)}, []byte(":")))
}

func decodeTransactionQuery(ticket []byte) (txnID, query string, err error) {
	fmt.Println("DBG:: COND 103")
	id, queryBytes, found := bytes.Cut(ticket, []byte(":"))
	if !found {
		err = fmt.Errorf("%w: malformed ticket", arrow.ErrInvalid)
		return
	}
	txnID = string(id)
	query = string(queryBytes)
	return
}

type Statement struct {
	stmt   *sql.Stmt
	params [][]interface{}
}

type DuckDBFlightSQLServer struct {
	flightsql.BaseServer
	db *sql.DB

	prepared         sync.Map
	openTransactions sync.Map
}

func NewDuckDBFlightSQLServer(db *sql.DB) (*DuckDBFlightSQLServer, error) {
	fmt.Println("DBG:: COND 1*")
	ret := &DuckDBFlightSQLServer{db: db}
	ret.Alloc = memory.DefaultAllocator
	for k, v := range SqlInfoResultMap() {
		ret.RegisterSqlInfo(flightsql.SqlInfo(k), v)
	}
	return ret, nil
}

func (s *DuckDBFlightSQLServer) flightInfoForCommand(desc *flight.FlightDescriptor, schema *arrow.Schema) *flight.FlightInfo {
	fmt.Println("DBG:: COND 202")
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, s.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func (f *DuckDBFlightSQLServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 2*")
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "flight descriptor required")
	}
	return nil, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 31")
	query, txnid := cmd.GetQuery(), cmd.GetTransactionId()
	tkt, err := encodeTransactionQuery(query, txnid)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (s *DuckDBFlightSQLServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fmt.Println("DBG:: COND 32")
	txnid, query, err := decodeTransactionQuery(cmd.GetStatementHandle())
	if err != nil {
		return nil, nil, err
	}

	var db dbQueryCtx = s.db
	if txnid != "" {
		tx, loaded := s.openTransactions.Load(txnid)
		if !loaded {
			return nil, nil, fmt.Errorf("%w: invalid transaction id specified: %s", arrow.ErrInvalid, txnid)
		}
		db = tx.(*sql.Tx)
	}

	return doGetQuery(ctx, s.Alloc, db, query, nil)
}

func (s *DuckDBFlightSQLServer) GetFlightInfoCatalogs(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 33")
	return s.flightInfoForCommand(desc, schema_ref.Catalogs), nil
}

func (s *DuckDBFlightSQLServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// https://www.sqlite.org/cli.html
	// > The ".databases" command shows a list of all databases open
	// > in the current connection. There will always be at least
	// > 2. The first one is "main", the original database opened. The
	// > second is "temp", the database used for temporary tables.
	// For our purposes, return only "main" and ignore other databases.
	fmt.Println("DBG:: COND 34")
	schema := schema_ref.Catalogs

	catalogs, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
	if err != nil {
		return nil, nil, err
	}
	defer catalogs.Release()

	batch := array.NewRecord(schema, []arrow.Array{catalogs}, 1)

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)

	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoSubstraitPlan(ctx context.Context, ssp flightsql.StatementSubstraitPlan, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 35")
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoSchemas(_ context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 36")
	return s.flightInfoForCommand(desc, schema_ref.DBSchemas), nil
}

func (s *DuckDBFlightSQLServer) DoGetDBSchemas(_ context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// SQLite doesn't support schemas, so pretend we have a single unnamed schema.
	fmt.Println("DBG:: COND 37")
	schema := schema_ref.DBSchemas

	ch := make(chan flight.StreamChunk, 1)

	if cmd.GetDBSchemaFilterPattern() == nil || *cmd.GetDBSchemaFilterPattern() == "" {
		catalogs, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
		if err != nil {
			return nil, nil, err
		}
		defer catalogs.Release()

		dbSchemas, _, err := array.FromJSON(s.Alloc, arrow.BinaryTypes.String, strings.NewReader(`[""]`))
		if err != nil {
			return nil, nil, err
		}
		defer dbSchemas.Release()

		batch := array.NewRecord(schema, []arrow.Array{catalogs, dbSchemas}, 1)
		ch <- flight.StreamChunk{Data: batch}
	}

	close(ch)

	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoTables(_ context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 38")
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return s.flightInfoForCommand(desc, schema), nil
}

func (s *DuckDBFlightSQLServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fmt.Println("DBG:: COND 39")
	// query := prepareQueryForGetTables(cmd)
	query := "SHOW ALL TABLES"
	fmt.Println("query", query)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, nil, err
	}

	var rdr array.RecordReader

	rdr, err = NewSqlBatchReaderWithSchema(s.Alloc, schema_ref.Tables, rows)
	if err != nil {
		fmt.Println("error", err.Error())
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk, 2)
	if cmd.GetIncludeSchema() {
		rdr, err = NewSqliteTablesSchemaBatchReader(ctx, s.Alloc, rdr, s.db, query)
		if err != nil {
			fmt.Println("error", err.Error())
			return nil, nil, err
		}
	}

	schema := rdr.Schema()
	go flight.StreamChunksFromReader(rdr, ch)
	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoXdbcTypeInfo(_ context.Context, _ flightsql.GetXdbcTypeInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 40")
	return s.flightInfoForCommand(desc, schema_ref.XdbcTypeInfo), nil
}

func (s *DuckDBFlightSQLServer) DoGetXdbcTypeInfo(_ context.Context, cmd flightsql.GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fmt.Println("DBG:: COND 41")
	var batch arrow.Record
	if cmd.GetDataType() == nil {
		batch = GetTypeInfoResult(s.Alloc)
	} else {
		batch = GetFilteredTypeInfoResult(s.Alloc, *cmd.GetDataType())
	}

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return batch.Schema(), ch, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoTableTypes(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 42")
	return s.flightInfoForCommand(desc, schema_ref.TableTypes), nil
}

func (s *DuckDBFlightSQLServer) DoGetTableTypes(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fmt.Println("DBG:: COND 43")
	query := "SELECT DISTINCT type AS table_type FROM sqlite_master"
	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.TableTypes)
}

func (s *DuckDBFlightSQLServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	fmt.Println("DBG:: COND 44")
	var (
		res sql.Result
		err error
	)

	if len(cmd.GetTransactionId()) > 0 {
		tx, loaded := s.openTransactions.Load(string(cmd.GetTransactionId()))
		if !loaded {
			return -1, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}

		res, err = tx.(*sql.Tx).ExecContext(ctx, cmd.GetQuery())
	} else {
		res, err = s.db.ExecContext(ctx, cmd.GetQuery())
	}

	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *DuckDBFlightSQLServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	fmt.Println("DBG:: COND 45")
	var stmt *sql.Stmt

	if len(req.GetTransactionId()) > 0 {
		tx, loaded := s.openTransactions.Load(string(req.GetTransactionId()))
		if !loaded {
			return result, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
		}
		stmt, err = tx.(*sql.Tx).PrepareContext(ctx, req.GetQuery())
	} else {
		stmt, err = s.db.PrepareContext(ctx, req.GetQuery())
	}

	if err != nil {
		return result, err
	}

	handle := genRandomString()
	s.prepared.Store(string(handle), Statement{stmt: stmt})

	result.Handle = handle
	// no way to get the dataset or parameter schemas from sql.DB
	return
}

func (s *DuckDBFlightSQLServer) CreatePreparedSubstraitPlan(ctx context.Context, req flightsql.ActionCreatePreparedSubstraitPlanRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	fmt.Println("DBG:: COND 46")

	var stmt *sql.Stmt
	query := string(req.GetPlan().Plan)

	fmt.Println("query: ", query)

	// if len(req.GetTransactionId()) > 0 {
	// 	fmt.Println("1")
	// 	tx, loaded := s.openTransactions.Load(string(req.GetTransactionId()))
	// 	if !loaded {
	// 		return result, status.Error(codes.InvalidArgument, "invalid transaction handle provided")
	// 	}
	// 	stmt, err = tx.(*sql.Tx).PrepareContext(ctx, query)
	// 	if err != nil {
	// 		fmt.Println(err.Error())
	// 	}
	// } else {
	stmt, err = s.db.PrepareContext(ctx, query)
	if err != nil {
		fmt.Println(err.Error())
	}
	// }
	handle := genRandomString()
	s.prepared.Store(string(handle), Statement{stmt: stmt})
	fmt.Println("End of CreatePreparedSubstraitPlan")
	result.Handle = handle
	// no way to get the dataset or parameter schemas from sql.DB
	return
}

func (s *DuckDBFlightSQLServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	fmt.Println("DBG:: COND 1")
	handle := request.GetPreparedStatementHandle()
	if val, loaded := s.prepared.LoadAndDelete(string(handle)); loaded {
		stmt := val.(Statement)
		return stmt.stmt.Close()
	}

	return status.Error(codes.InvalidArgument, "prepared statement not found")
}

func (s *DuckDBFlightSQLServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 2")
	_, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

type dbQueryCtx interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func doGetQuery(ctx context.Context, mem memory.Allocator, db dbQueryCtx, query string, schema *arrow.Schema, args ...interface{}) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	fmt.Println("DBG:: COND 101")
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		// Not really useful except for testing Flight SQL clients
		trailers := metadata.Pairs("afsql-sqlite-query", query)
		grpc.SetTrailer(ctx, trailers)
		return nil, nil, err
	}

	var rdr *SqlBatchReader
	if schema != nil {
		rdr, err = NewSqlBatchReaderWithSchema(mem, schema, rows)
	} else {
		rdr, err = NewSqlBatchReader(mem, rows)
		if err == nil {
			schema = rdr.schema
		}
	}

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go flight.StreamChunksFromReader(rdr, ch)
	return schema, ch, nil
}

func (s *DuckDBFlightSQLServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (schema *arrow.Schema, out <-chan flight.StreamChunk, err error) {
	fmt.Println("DBG:: COND 3")
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return nil, nil, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	readers := make([]array.RecordReader, 0, len(stmt.params))
	if len(stmt.params) == 0 {
		fmt.Println("1")
		rows, err := stmt.stmt.QueryContext(ctx)
		if err != nil {
			fmt.Println("eerr1", err.Error())
			return nil, nil, err
		}

		rdr, err := NewSqlBatchReader(s.Alloc, rows)
		if err != nil {
			fmt.Println("error", err.Error())
			return nil, nil, err
		}

		schema = rdr.schema
		readers = append(readers, rdr)
	} else {
		fmt.Println("2")
		defer func() {
			if err != nil {
				for _, r := range readers {
					r.Release()
				}
			}
		}()
		var (
			rows *sql.Rows
			rdr  *SqlBatchReader
		)
		// if we have multiple rows of bound params, execute the query
		// multiple times and concatenate the result sets.
		for _, p := range stmt.params {
			rows, err = stmt.stmt.QueryContext(ctx, p...)
			if err != nil {
				return nil, nil, err
			}

			if schema == nil {
				fmt.Println("3")
				rdr, err = NewSqlBatchReader(s.Alloc, rows)
				if err != nil {
					return nil, nil, err
				}
				schema = rdr.schema
			} else {
				fmt.Println("4")
				rdr, err = NewSqlBatchReaderWithSchema(s.Alloc, schema, rows)
				if err != nil {
					return nil, nil, err
				}
			}

			readers = append(readers, rdr)
		}
	}

	ch := make(chan flight.StreamChunk)
	go flight.ConcatenateReaders(readers, ch)
	out = ch
	return
}

func scalarToIFace(s scalar.Scalar) (interface{}, error) {
	fmt.Println("DBG:: COND 205")
	if !s.IsValid() {
		return nil, nil
	}

	switch val := s.(type) {
	case *scalar.Int8:
		return val.Value, nil
	case *scalar.Uint8:
		return val.Value, nil
	case *scalar.Int32:
		return val.Value, nil
	case *scalar.Int64:
		return val.Value, nil
	case *scalar.Float32:
		return val.Value, nil
	case *scalar.Float64:
		return val.Value, nil
	case *scalar.String:
		return string(val.Value.Bytes()), nil
	case *scalar.Binary:
		return val.Value.Bytes(), nil
	case scalar.DateScalar:
		return val.ToTime(), nil
	case scalar.TimeScalar:
		return val.ToTime(), nil
	case *scalar.DenseUnion:
		return scalarToIFace(val.Value)
	default:
		return nil, fmt.Errorf("unsupported type: %s", val)
	}
}

func getParamsForStatement(rdr flight.MessageReader) (params [][]interface{}, err error) {
	fmt.Println("DBG:: COND 206")
	params = make([][]interface{}, 0)
	for rdr.Next() {
		rec := rdr.Record()

		nrows := int(rec.NumRows())
		ncols := int(rec.NumCols())

		for i := 0; i < nrows; i++ {
			invokeParams := make([]interface{}, ncols)
			for c := 0; c < ncols; c++ {
				col := rec.Column(c)
				sc, err := scalar.GetScalar(col, i)
				if err != nil {
					return nil, err
				}
				if r, ok := sc.(scalar.Releasable); ok {
					r.Release()
				}

				invokeParams[c], err = scalarToIFace(sc)
				if err != nil {
					return nil, err
				}
			}
			params = append(params, invokeParams)
		}
	}

	return params, rdr.Err()
}

func (s *DuckDBFlightSQLServer) DoPutPreparedStatementQuery(_ context.Context, cmd flightsql.PreparedStatementQuery, rdr flight.MessageReader, _ flight.MetadataWriter) error {
	fmt.Println("DBG:: COND 4")
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return status.Errorf(codes.Internal, "error gathering parameters for prepared statement query: %s", err.Error())
	}

	stmt.params = args
	s.prepared.Store(string(cmd.GetPreparedStatementHandle()), stmt)
	return nil
}

func (s *DuckDBFlightSQLServer) DoPutPreparedStatementUpdate(ctx context.Context, cmd flightsql.PreparedStatementUpdate, rdr flight.MessageReader) (int64, error) {
	fmt.Println("DBG:: COND 5")
	val, ok := s.prepared.Load(string(cmd.GetPreparedStatementHandle()))
	if !ok {
		return 0, status.Error(codes.InvalidArgument, "prepared statement not found")
	}

	stmt := val.(Statement)
	args, err := getParamsForStatement(rdr)
	if err != nil {
		return 0, status.Errorf(codes.Internal, "error gathering parameters for prepared statement: %s", err.Error())
	}

	if len(args) == 0 {
		result, err := stmt.stmt.ExecContext(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				return 0, status.Error(codes.NotFound, err.Error())
			}
			return 0, err
		}

		return result.RowsAffected()
	}

	var totalAffected int64
	for _, p := range args {
		result, err := stmt.stmt.ExecContext(ctx, p...)
		if err != nil {
			if strings.Contains(err.Error(), "no such table") {
				return totalAffected, status.Error(codes.NotFound, err.Error())
			}
			return totalAffected, err
		}

		n, err := result.RowsAffected()
		if err != nil {
			return totalAffected, err
		}
		totalAffected += n
	}

	return totalAffected, nil
}

func (s *DuckDBFlightSQLServer) GetFlightInfoPrimaryKeys(_ context.Context, cmd flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 6")
	return s.flightInfoForCommand(desc, schema_ref.PrimaryKeys), nil
}

func (s *DuckDBFlightSQLServer) DoGetPrimaryKeys(ctx context.Context, cmd flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// the field key_name cannot be recovered by sqlite so it is
	// being set to null following the same pattern for catalog name and schema_name
	fmt.Println("DBG:: COND 7")
	var b strings.Builder

	b.WriteString(`
	SELECT null AS catalog_name, null AS schema_name, table_name, name AS column_name, pk AS key_sequence, null as key_name
	FROM pragma_table_info(table_name)
		JOIN (SELECT null AS catalog_name, null AS schema_name, name AS table_name, type AS table_type
			FROM sqlite_master) where 1=1 AND pk !=0`)

	if cmd.Catalog != nil {
		fmt.Fprintf(&b, " and catalog_name LIKE '%s'", *cmd.Catalog)
	}
	if cmd.DBSchema != nil {
		fmt.Fprintf(&b, " and schema_name LIKE '%s'", *cmd.DBSchema)
	}

	fmt.Fprintf(&b, " and table_name LIKE '%s'", cmd.Table)

	return doGetQuery(ctx, s.Alloc, s.db, b.String(), schema_ref.PrimaryKeys)
}

func (s *DuckDBFlightSQLServer) GetFlightInfoImportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 8")
	return s.flightInfoForCommand(desc, schema_ref.ImportedKeys), nil
}

// func (s *DuckDBFlightSQLServer) DoGetImportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
// 	fmt.Println("DBG:: COND 9")
// 	filter := "fk_table_name = '" + ref.Table + "'"
// 	if ref.Catalog != nil {
// 		filter += " AND fk_catalog_name = '" + *ref.Catalog + "'"
// 	}
// 	if ref.DBSchema != nil {
// 		filter += " AND fk_schema_name = '" + *ref.DBSchema + "'"
// 	}
// 	query := prepareQueryForGetKeys(filter)
// 	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ImportedKeys)
// }

func (s *DuckDBFlightSQLServer) GetFlightInfoExportedKeys(_ context.Context, _ flightsql.TableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 10")
	return s.flightInfoForCommand(desc, schema_ref.ExportedKeys), nil
}

// func (s *DuckDBFlightSQLServer) DoGetExportedKeys(ctx context.Context, ref flightsql.TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
// 	fmt.Println("DBG:: COND 11")
// 	filter := "pk_table_name = '" + ref.Table + "'"
// 	if ref.Catalog != nil {
// 		filter += " AND pk_catalog_name = '" + *ref.Catalog + "'"
// 	}
// 	if ref.DBSchema != nil {
// 		filter += " AND pk_schema_name = '" + *ref.DBSchema + "'"
// 	}
// 	query := prepareQueryForGetKeys(filter)
// 	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ExportedKeys)
// }

func (s *DuckDBFlightSQLServer) GetFlightInfoCrossReference(_ context.Context, _ flightsql.CrossTableRef, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	fmt.Println("DBG:: COND 12")
	return s.flightInfoForCommand(desc, schema_ref.CrossReference), nil
}

// func (s *DuckDBFlightSQLServer) DoGetCrossReference(ctx context.Context, cmd flightsql.CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
// 	fmt.Println("DBG:: COND 13")
// 	pkref := cmd.PKRef
// 	filter := "pk_table_name = '" + pkref.Table + "'"
// 	if pkref.Catalog != nil {
// 		filter += " AND pk_catalog_name = '" + *pkref.Catalog + "'"
// 	}
// 	if pkref.DBSchema != nil {
// 		filter += " AND pk_schema_name = '" + *pkref.DBSchema + "'"
// 	}

// 	fkref := cmd.FKRef
// 	filter += " AND fk_table_name = '" + fkref.Table + "'"
// 	if fkref.Catalog != nil {
// 		filter += " AND fk_catalog_name = '" + *fkref.Catalog + "'"
// 	}
// 	if fkref.DBSchema != nil {
// 		filter += " AND fk_schema_name = '" + *fkref.DBSchema + "'"
// 	}
// 	query := prepareQueryForGetKeys(filter)
// 	return doGetQuery(ctx, s.Alloc, s.db, query, schema_ref.ExportedKeys)
// }

func (s *DuckDBFlightSQLServer) BeginTransaction(_ context.Context, req flightsql.ActionBeginTransactionRequest) (id []byte, err error) {
	fmt.Println("DBG:: COND 14")
	tx, err := s.db.Begin()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to begin transaction: %s", err.Error())
	}

	handle := genRandomString()
	s.openTransactions.Store(string(handle), tx)
	return handle, nil
}

func (s *DuckDBFlightSQLServer) EndTransaction(_ context.Context, req flightsql.ActionEndTransactionRequest) error {
	fmt.Println("DBG:: COND 15")
	if req.GetAction() == flightsql.EndTransactionUnspecified {
		return status.Error(codes.InvalidArgument, "must specify Commit or Rollback to end transaction")
	}

	handle := string(req.GetTransactionId())
	if tx, loaded := s.openTransactions.LoadAndDelete(handle); loaded {
		txn := tx.(*sql.Tx)
		switch req.GetAction() {
		case flightsql.EndTransactionCommit:
			if err := txn.Commit(); err != nil {
				return status.Error(codes.Internal, "failed to commit transaction: "+err.Error())
			}
		case flightsql.EndTransactionRollback:
			if err := txn.Rollback(); err != nil {
				return status.Error(codes.Internal, "failed to rollback transaction: "+err.Error())
			}
		}
		return nil
	}

	return status.Error(codes.InvalidArgument, "transaction id not found")
}
