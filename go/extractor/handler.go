package extractor

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// ExtractorHandler handles extracting table metadata
func ExtractorHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/extractor" {
		http.Error(w, "404 page not found :)", http.StatusNotFound)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Method is not supported", http.StatusNotFound)
		return
	}

	dsn, err := formatDSN(r.URL.Query())
	if err != nil {
		http.Error(w, fmt.Sprintf("DSN could not be formed: %s", err.Error()), http.StatusBadRequest)
		return
	}

	extractor, err := NewExtractor(dsn)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not create extractor: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	defer extractor.CloseDB()

	isPublic := true
	if r.URL.Query().Get("public") == "false" {
		isPublic = false
	}

	metadatas, err := extractor.GetTableMetadata(isPublic)

	if err != nil {
		http.Error(w, err.Error(), http.StatusTeapot)
	}

	if err := json.NewEncoder(w).Encode(metadatas); err != nil {
		http.Error(w, err.Error(), http.StatusExpectationFailed)
	}

	return
}

// formatDSN is a helper function to return a formatted DSN string
func formatDSN(queryVals url.Values) (string, error) {
	host := queryVals.Get("host")
	port := queryVals.Get("port")
	user := queryVals.Get("username")
	password := queryVals.Get("password")
	dbname := queryVals.Get("dbname")

	if host == "" || port == "" || user == "" || password == "" || dbname == "" {
		return "", errors.New("dsn could not be formed, check your params for missing arguments")
	}

	dsnString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	return dsnString, nil
}
