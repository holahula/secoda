package main

import (
	"database/sql"
)

type Executor struct {
	db *sql.DB
}

func NewExtractor(db *sql.DB) *Executor {

	return &Executor{
		db: db,
	}
}

func (e *Executor) GetTableNames() ([]string, error) {
	q := `SELECT table_name FROM information_schema.tables WHERE table_schema = $1`

	rows, err := e.db.Query(q, "public")
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
