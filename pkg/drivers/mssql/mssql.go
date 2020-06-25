package mssql

import (
	"context"
	cryptotls "crypto/tls"
	"database/sql"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/rancher/kine/pkg/server"
	"github.com/rancher/kine/pkg/tls"
	"net/url"
)

const (
	defaultDSN = "sqlserver://sa:"
)

var (
	schema = []string {
		`if not exists (
			select *
			from sys.tables t
			join sys.schemas s
			on (
				t.schema_id = s.schema_id
			) where
			s.name = @SchemaName and
			t.name = kine
		) begin create table kine (
			id int primary key identity (1, 1),
			name varchar(630),
			created int,
			deleted int,
			create_revision int,
			prev_revision int,
			lease int,
			value binary(max),
			old_value binary(max)
		) end;
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_name_index' and
			object_id = OBJECT_ID('kine')
		) begin
		create nonclustered index kine_name_index on kine (kine.name)
		end;
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_name_prev_revision_uindex' and
			object_id = OBJECT_ID('kine')
		) begin
		create unique index kine_name_prev_revision_uindex on kine (kine.name, kine.prev_revision)
		`,
	}
	createDB = "create database "
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config) (server.Backend, error) {
	dsn, err := buildDsn(dataSourceName, tlsInfo)
	if err != nil {
		return nil, err
	}


}

func generateConnector(dataSourceName string) (*mssql.Connector, error) {
	conn, err := mssql.NewConnector(dataSourceName)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createDBIfNotExist(dataSourceName string) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := u.Query().Get("database")
	db, err := sql.Open("sqlserver", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if _, ok := err.(*mssql.Error); !ok {
		return err
	}

	if err := err.(*mssql.Error); err.Number != 1801 { // 1801 = database already exists
		db, err := sql.Open("sqlserver", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(createDB + dbName + ":")
		if err != nil {
			return err
		}
	}
	return nil
}

func buildDsn(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		return "", fmt.Errorf("invalid dsn")
	} else {
		dataSourceName = "sqlserver://" + dataSourceName
	}

	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}

	queryMap := u.Query()
	params := url.Values{}

	if _, ok := queryMap["certificate"]; tlsInfo.CertFile != "" && !ok {
		params.Add("certificate", tlsInfo.CAFile)
	}

	if _, ok := queryMap["database"]; !ok {
		params.Add("database", "kubernetes")
	}

	for k, v := range queryMap {
		params.Add(k, v[0])
	}

	u.RawQuery = params.Encode()
	return u.String(), nil
}