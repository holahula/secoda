package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

const (
	port string = ":8080"
)

func extractorHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/extractor" {

		http.Error(w, "404 not found", http.StatusNotFound)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Method is not supported", http.StatusNotFound)
		return
	}

	fmt.Fprintf(w, "howdy")

}

func main() {
	db, err := sql.Open("postgres", "postgres://test:test@127.0.0.1/test")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	http.HandleFunc("/extractor", extractorHandler)

	fmt.Println("listening at", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}
