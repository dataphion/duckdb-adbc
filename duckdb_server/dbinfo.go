package duckdb_server

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql"
)

func SqlInfoResultMap() flightsql.SqlInfoResultMap {
	return flightsql.SqlInfoResultMap{
		uint32(flightsql.SqlInfoFlightSqlServerName):         "Dataphion Duck DB",
		uint32(flightsql.SqlInfoFlightSqlServerVersion):      "Duck DB",
		uint32(flightsql.SqlInfoFlightSqlServerArrowVersion): arrow.PkgVersion,
		uint32(flightsql.SqlInfoFlightSqlServerReadOnly):     false,
		uint32(flightsql.SqlInfoDDLCatalog):                  false,
		uint32(flightsql.SqlInfoDDLSchema):                   false,
		uint32(flightsql.SqlInfoDDLTable):                    true,
		uint32(flightsql.SqlInfoIdentifierCase):              int64(flightsql.SqlCaseSensitivityCaseInsensitive),
		uint32(flightsql.SqlInfoIdentifierQuoteChar):         `"`,
		uint32(flightsql.SqlInfoQuotedIdentifierCase):        int64(flightsql.SqlCaseSensitivityCaseInsensitive),
		uint32(flightsql.SqlInfoAllTablesAreASelectable):     true,
		uint32(flightsql.SqlInfoNullOrdering):                int64(flightsql.SqlNullOrderingSortAtStart),
		uint32(flightsql.SqlInfoFlightSqlServerTransaction):  int32(flightsql.SqlTransactionTransaction),
		uint32(flightsql.SqlInfoTransactionsSupported):       true,
		uint32(flightsql.SqlInfoKeywords): []string{"ABORT",
			"ACTION",
			"ADD",
			"AFTER",
			"ALL",
			"ALTER",
			"ALWAYS",
			"ANALYZE",
			"AND",
			"AS",
			"ASC",
			"ATTACH",
			"AUTOINCREMENT",
			"BEFORE",
			"BEGIN",
			"BETWEEN",
			"BY",
			"CASCADE",
			"CASE",
			"CAST",
			"CHECK",
			"COLLATE",
			"COLUMN",
			"COMMIT",
			"CONFLICT",
			"CONSTRAINT",
			"CREATE",
			"CROSS",
			"CURRENT",
			"CURRENT_DATE",
			"CURRENT_TIME",
			"CURRENT_TIMESTAMP",
			"DATABASE",
			"DEFAULT",
			"DEFERRABLE",
			"DEFERRED",
			"DELETE",
			"DESC",
			"DETACH",
			"DISTINCT",
			"DO",
			"DROP",
			"EACH",
			"ELSE",
			"END",
			"ESCAPE",
			"EXCEPT",
			"EXCLUDE",
			"EXCLUSIVE",
			"EXISTS",
			"EXPLAIN",
			"FAIL",
			"FILTER",
			"FIRST",
			"FOLLOWING",
			"FOR",
			"FOREIGN",
			"FROM",
			"FULL",
			"GENERATED",
			"GLOB",
			"GROUP",
			"GROUPS",
			"HAVING",
			"IF",
			"IGNORE",
			"IMMEDIATE",
			"IN",
			"INDEX",
			"INDEXED",
			"INITIALLY",
			"INNER",
			"INSERT",
			"INSTEAD",
			"INTERSECT",
			"INTO",
			"IS",
			"ISNULL",
			"JOIN",
			"KEY",
			"LAST",
			"LEFT",
			"LIKE",
			"LIMIT",
			"MATCH",
			"MATERIALIZED",
			"NATURAL",
			"NO",
			"NOT",
			"NOTHING",
			"NOTNULL",
			"NULL",
			"NULLS",
			"OF",
			"OFFSET",
			"ON",
			"OR",
			"ORDER",
			"OTHERS",
			"OUTER",
			"OVER",
			"PARTITION",
			"PLAN",
			"PRAGMA",
			"PRECEDING",
			"PRIMARY",
			"QUERY",
			"RAISE",
			"RANGE",
			"RECURSIVE",
			"REFERENCES",
			"REGEXP",
			"REINDEX",
			"RELEASE",
			"RENAME",
			"REPLACE",
			"RESTRICT",
			"RETURNING",
			"RIGHT",
			"ROLLBACK",
			"ROW",
			"ROWS",
			"SAVEPOINT",
			"SELECT",
			"SET",
			"TABLE",
			"TEMP",
			"TEMPORARY",
			"THEN",
			"TIES",
			"TO",
			"TRANSACTION",
			"TRIGGER",
			"UNBOUNDED",
			"UNION",
			"UNIQUE",
			"UPDATE",
			"USING",
			"VACUUM",
			"VALUES",
			"VIEW",
			"VIRTUAL",
			"WHEN",
			"WHERE",
			"WINDOW",
			"WITH",
			"WITHOUT"},
		uint32(flightsql.SqlInfoNumericFunctions): []string{
			"ACOS", "ACOSH", "ASIN", "ASINH", "ATAN", "ATAN2", "ATANH", "CEIL",
			"CEILING", "COS", "COSH", "DEGREES", "EXP", "FLOOR", "LN", "LOG",
			"LOG10", "LOG2", "MOD", "PI", "POW", "POWER", "RADIANS",
			"SIN", "SINH", "SQRT", "TAN", "TANH", "TRUNC"},
		uint32(flightsql.SqlInfoStringFunctions): []string{"SUBSTR", "TRIM", "LTRIM", "RTRIM", "LENGTH",
			"REPLACE", "UPPER", "LOWER", "INSTR"},
		uint32(flightsql.SqlInfoSupportsConvert): map[int32][]int32{
			int32(flightsql.SqlConvertBigInt): {int32(flightsql.SqlConvertInteger)},
		},
	}
}