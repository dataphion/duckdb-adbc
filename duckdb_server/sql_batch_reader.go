//go:build go1.18
// +build go1.18

package duckdb_server

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func getArrowTypeFromString(dbtype string) arrow.DataType {
	fmt.Println("gggggggggggreeeeeeeeeeeeetttttttttt", dbtype)
	dbtype = strings.ToLower(dbtype)
	if dbtype == "" {
		// SQLite may not know the type yet.
		return &arrow.NullType{}
	}

	if dbtype == "varchar[]" {
		// Return arrow.ListOf(arrow.BinaryTypes.String)
		return arrow.ListOf(arrow.BinaryTypes.String)
	}
	if strings.HasPrefix(dbtype, "varchar") {
		return arrow.BinaryTypes.String
	}

	switch dbtype {
	case "tinyint":
		return arrow.PrimitiveTypes.Int8
	case "mediumint":
		return arrow.PrimitiveTypes.Int32
	case "int", "integer", "bigint", "int64":
		return arrow.PrimitiveTypes.Int64
	case "float":
		return arrow.PrimitiveTypes.Float32
	case "real", "double":
		return arrow.PrimitiveTypes.Float64
	case "blob":
		return arrow.BinaryTypes.Binary
	case "text", "date", "char", "clob":
		return arrow.BinaryTypes.String
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	default:
		panic("invalid sqlite type: " + dbtype)
	}
}

var sqliteDenseUnion = arrow.DenseUnionOf([]arrow.Field{
	{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
}, []arrow.UnionTypeCode{0, 1, 2})

func getArrowType(c *sql.ColumnType) arrow.DataType {
	dbtype := strings.ToLower(c.DatabaseTypeName())
	fmt.Println("dddddddddddddbbbbbbbbbbbbbbbbbbbtttttttttttt", dbtype)
	if dbtype == "" {
		if c.ScanType() == nil {
			return sqliteDenseUnion
		}
		switch c.ScanType().Kind() {
		case reflect.Int8, reflect.Uint8:
			return arrow.PrimitiveTypes.Int8
		case reflect.Int32, reflect.Uint32:
			return arrow.PrimitiveTypes.Int32
		case reflect.Int, reflect.Int64, reflect.Uint64:
			return arrow.PrimitiveTypes.Int64
		case reflect.Float32:
			return arrow.PrimitiveTypes.Float32
		case reflect.Float64:
			return arrow.PrimitiveTypes.Float64
		case reflect.String:
			return arrow.BinaryTypes.String
		case reflect.Bool:
			return arrow.FixedWidthTypes.Boolean
		}
	}
	return getArrowTypeFromString(dbtype)
}

const maxBatchSize = 1024

type SqlBatchReader struct {
	refCount int64

	schema *arrow.Schema
	rows   *sql.Rows
	record arrow.Record
	bldr   *array.RecordBuilder
	err    error

	rowdest []interface{}
}

func NewSqlBatchReaderWithSchema(mem memory.Allocator, schema *arrow.Schema, rows *sql.Rows) (*SqlBatchReader, error) {
	rowdest := make([]interface{}, schema.NumFields())
	for i, f := range schema.Fields() {
		switch f.Type.ID() {
		case arrow.DENSE_UNION, arrow.SPARSE_UNION:
			rowdest[i] = new(interface{})
		case arrow.UINT8, arrow.INT8:
			if f.Nullable {
				rowdest[i] = &sql.NullByte{}
			} else {
				rowdest[i] = new(uint8)
			}
		case arrow.INT32:
			if f.Nullable {
				rowdest[i] = &sql.NullInt32{}
			} else {
				rowdest[i] = new(int32)
			}
		case arrow.INT64:
			if f.Nullable {
				rowdest[i] = &sql.NullInt64{}
			} else {
				rowdest[i] = new(int64)
			}
		case arrow.FLOAT32, arrow.FLOAT64:
			if f.Nullable {
				rowdest[i] = &sql.NullFloat64{}
			} else {
				rowdest[i] = new(float64)
			}
		case arrow.BINARY:
			var b []byte
			rowdest[i] = &b
		case arrow.STRING:
			if f.Nullable {
				rowdest[i] = &sql.NullString{}
			} else {
				rowdest[i] = new(string)
			}
		}
	}

	return &SqlBatchReader{
		refCount: 1,
		bldr:     array.NewRecordBuilder(mem, schema),
		schema:   schema,
		rowdest:  rowdest,
		rows:     rows}, nil
}

func NewSqlBatchReader(mem memory.Allocator, rows *sql.Rows) (*SqlBatchReader, error) {
	// bldr := flightsql.NewColumnMetadataBuilder()

	cols, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		println("eerrrwre", err.Error())
		return nil, err
	}

	rowdest := make([]interface{}, len(cols))
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fmt.Println("Inside cols reader...")
		fields[i].Name = c.Name()
		if c.Name() == "?" {
			fields[i].Name += ":" + strconv.Itoa(i)
		}
		fields[i].Nullable, _ = c.Nullable()
		fields[i].Type = getArrowType(c)
		// fields[i].Metadata = getColumnMetadata(bldr, getSqlTypeFromTypeName(c.DatabaseTypeName()), "")
		switch fields[i].Type.ID() {
		case arrow.DENSE_UNION, arrow.SPARSE_UNION:
			rowdest[i] = new(interface{})
		case arrow.UINT8, arrow.INT8:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullByte{}
			} else {
				rowdest[i] = new(uint8)
			}
		case arrow.INT32:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullInt32{}
			} else {
				rowdest[i] = new(int32)
			}
		case arrow.INT64:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullInt64{}
			} else {
				rowdest[i] = new(int64)
			}
		case arrow.FLOAT64, arrow.FLOAT32:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullFloat64{}
			} else {
				rowdest[i] = new(float64)
			}
		case arrow.BINARY:
			var b []byte
			rowdest[i] = &b
		case arrow.BOOL:
			var b bool
			rowdest[i] = &b
		case arrow.STRING:
			if fields[i].Nullable {
				rowdest[i] = &sql.NullString{}
			} else {
				rowdest[i] = new(string)
			}
		case arrow.LIST:
			fmt.Println("*** LIST")
			var b []interface{}
			rowdest[i] = &b
		}
	}

	schema := arrow.NewSchema(fields, nil)
	return &SqlBatchReader{
		refCount: 1,
		bldr:     array.NewRecordBuilder(mem, schema),
		schema:   schema,
		rowdest:  rowdest,
		rows:     rows}, nil
}

func (r *SqlBatchReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *SqlBatchReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		r.rows.Close()
		r.rows, r.schema, r.rowdest = nil, nil, nil
		r.bldr.Release()
		r.bldr = nil
		if r.record != nil {
			r.record.Release()
			r.record = nil
		}
	}
}
func (r *SqlBatchReader) Schema() *arrow.Schema { return r.schema }

func (r *SqlBatchReader) Record() arrow.Record { return r.record }

func (r *SqlBatchReader) Err() error { return r.err }

func (r *SqlBatchReader) Next() bool {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}

	rows := 0
	for rows < maxBatchSize && r.rows.Next() {
		if err := r.rows.Scan(r.rowdest...); err != nil {
			fmt.Println("errxxxxxxxx", err.Error())
			// Not really useful except for testing Flight SQL clients
			// detail := wrapperspb.StringValue{Value: r.schema.String()}
			// if st, sterr := status.New(codes.Unknown, err.Error()).WithDetails(&detail); sterr != nil {
			// 	// r.err = err
			// 	fmt.Println("err1", err.Error())
			// } else {
			// 	fmt.Println("err2", err.Error())
			// 	// r.err = st.Err()
			// }
			// return false
		}

		for i, v := range r.rowdest {
			fb := r.bldr.Field(i)
			fmt.Println(i, v)

			switch v := v.(type) {
			case *uint8:
				fb.(*array.Uint8Builder).Append(*v)
			case *sql.NullByte:
				if !v.Valid {
					fb.AppendNull()
				} else {
					fb.(*array.Uint8Builder).Append(v.Byte)
				}
			case *int64:
				if v == nil {
					fb.AppendEmptyValue()
				} else {
					fb.(*array.Int64Builder).Append(*v)
				}
			case *sql.NullInt64:
				if !v.Valid {
					fb.AppendNull()
				} else if v == nil {
					fb.AppendNull()
				} else {
					fb.(*array.Int64Builder).Append(v.Int64)
				}
			case *int32:
				fb.(*array.Int32Builder).Append(*v)
			case *sql.NullInt32:
				if !v.Valid {
					fb.AppendNull()
				} else {
					fb.(*array.Int32Builder).Append(v.Int32)
				}
			case *float64:
				switch b := fb.(type) {
				case *array.Float64Builder:
					b.Append(*v)
				case *array.Float32Builder:
					b.Append(float32(*v))
				}
			case *sql.NullFloat64:
				if !v.Valid {
					fb.AppendNull()
				} else {
					switch b := fb.(type) {
					case *array.Float64Builder:
						b.Append(v.Float64)
					case *array.Float32Builder:
						b.Append(float32(v.Float64))
					}
				}
			case *[]byte:
				if v == nil {
					fb.AppendNull()
				} else {
					fb.(*array.BinaryBuilder).Append(*v)
				}
			case *[]interface{}:
				fmt.Println("Insde Append Code...")
				// Append list of strings
				// stringData := make([]string, len(*v))
				b := make([]bool, len(*v))
				offsets := make([]int32, len(*v))
				for i := range *v {
					// stringData[i] = fmt.Sprint(j)
					b[i] = true
					offsets[i] = int32(i)
				}
				fb.(*array.ListBuilder).AppendValues(offsets, b)

			case *string:
				if v == nil {
					fb.AppendNull()
				} else {
					fb.(*array.StringBuilder).Append(*v)
				}
			case *sql.NullString:
				if !v.Valid {
					fb.AppendNull()
				} else {
					fb.(*array.StringBuilder).Append(v.String)
				}
			case *bool:
				fb.(*array.BooleanBuilder).Append(*v)
			default:
				fmt.Println(fmt.Sprintf("unsupported type %T", v))
			}

		}

		rows++
	}

	r.record = r.bldr.NewRecord()
	return rows > 0
}
