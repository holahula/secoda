package extractor

import (
	"database/sql"
	"fmt"
)

// Extractor extracts table metadata
type Extractor struct {
	db *sql.DB
}

// NewExtractor returns a new Extractor for a provided DSN
func NewExtractor(dsn string) (*Extractor, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return &Extractor{
		db: db,
	}, err
}

// CloseDB closes the DB connection. This should be deferred upon Extractor creation
func (e *Extractor) CloseDB() error {
	fmt.Println("closing DB connection")
	return e.db.Close()
}

// GetTableNames returns all the tables in the DB.
// If the public param is set to false, then private table information is returned.
func (e *Extractor) GetTableNames(isPublic bool) ([]string, error) {
	q := `SELECT tablename FROM pg_tables`

	if isPublic {
		q += " WHERE schemaname = 'public'"
	}

	rows, err := e.db.Query(q)
	if err != nil {
		return nil, err
	}

	var tableNames []string

	for rows.Next() {
		var name string

		if err := rows.Scan(
			&name,
		); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}

	return tableNames, nil
}

type TableMetadata struct {
	Columns   []*ColumnMetadata
	NumRows   int
	Schema    string
	Database  string
	TableName string
}

type ColumnMetadata struct {
	Name string
	Type string
}

// GetTableMetadata returns all the metadata of the provided DB
func (e *Extractor) GetTableMetadata(private bool) ([]*TableMetadata, error) {
	tableNames, err := e.GetTableNames(private)
	if err != nil {
		return nil, err
	}

	metadataQuery := `SELECT DISTINCT table_catalog, table_schema
			FROM information_schema.columns WHERE table_name = $1`

	colQuery := `SELECT column_name, data_type, character_maximum_length
			FROM information_schema.columns WHERE table_name = $1`

	rowCountQuery := `SELECT count(*) FROM %s`

	var metadatas []*TableMetadata

	for _, tableName := range tableNames {
		metadata := &TableMetadata{
			TableName: tableName,
			Columns:   []*ColumnMetadata{},
		}

		if err := e.db.QueryRow(metadataQuery, tableName).Scan(
			&metadata.Database,
			&metadata.Schema,
		); err != nil {
			printMetadataError(tableName, err)
			continue
		}

		rows, err := e.db.Query(colQuery, tableName)
		if err != nil {
			printMetadataError(tableName, err)
			continue
		}

		for rows.Next() {
			var column ColumnMetadata

			var dataType string
			var maxLength sql.NullString

			if err := rows.Scan(
				&column.Name,
				&dataType,
				&maxLength,
			); err != nil {
				return nil, err
			}

			column.Type = dataType
			if maxLength.Valid {
				column.Type += "(" + maxLength.String + ")"
			}

			metadata.Columns = append(metadata.Columns, &column)
		}

		if err := e.db.QueryRow(fmt.Sprintf(rowCountQuery, tableName)).Scan(
			&metadata.NumRows,
		); err != nil {
			printMetadataError(tableName, err)
			continue
		}

		metadatas = append(metadatas, metadata)
	}

	return metadatas, nil
}

func printMetadataError(tableName string, err error) {
	fmt.Printf("error retrieving table metadata for %s: %s\n", tableName, err.Error())
}
