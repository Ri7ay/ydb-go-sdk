package sugar

import "net/url"

// Usage of this package
//
// db, err := ydb.Open(
//   ctx,
//   sugar.DSN("endpoint", "database", false),
// )

// DSN makes connection string (data source name) by endpoint, database and secure
func DSN(endpoint, database string, secure bool) (s string) {
	qp := url.Values{}
	qp.Set("database", database)

	dsn := url.URL{
		Scheme:   "grpc",
		Host:     endpoint,
		RawQuery: qp.Encode(),
	}

	if secure {
		dsn.Scheme = "grpcs"
	}

	return dsn.String()
}
