package main

import (
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"

	"github.com/holahula/SecodaCodingChallenge/go/extractor"
)

const (
	port string = ":8080"
)

/*
	http://localhost:8080/extractor?host=<host>&port=<port>&username=<username>&password=<password>&dbname=<dbname>

	Test:
	1. Run extractor
	2. Spin up Postgres DB
	3. Create user / pwd / db, i.e. test / test / test
	4. Insert table
	5. Query @ http://localhost:8080/extractor?host=127.0.0.1&port=5432&username=test&password=test&dbname=test
*/

func main() {
	http.HandleFunc("/", extractor.ExtractorHandler)

	fmt.Println("listening at", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Println(err)
	}
}
