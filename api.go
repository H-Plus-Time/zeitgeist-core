package main

import (
	"fmt"
	"net/http"

	"github.com/go-gremlin/gremlin"
)

type Article struct {
	Pmid             string   `json:"pmid"`
	Title            string   `json:"full_title"`
	Pmc              string   `json:"pmc"`
	Doi              string   `json:"doi"`
	Publication_year string   `json:"publication_year"`
	Keywords         []string `json:"kwset"`
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Hello")
	fmt.Fprintf(w, "Welcome, %s!", r.URL.Path[1:])
}

func fetchArticles(w http.ResponseWriter, r *http.Request) {
	data, err := gremlin.Query(`g.V().hasLabel("article")`).Bindings(gremlin.Bind{}).Exec()
	if err != nil {

	}
	fmt.Fprintf(w, string(data))
}

func main() {
	if err := gremlin.NewCluster("ws://localhost:8182", "ws://staging.local:8182"); err != nil {
		// handle error
	}

	http.HandleFunc("/", handler)
	http.HandleFunc("/articles", fetchArticles)
	http.ListenAndServe(":8080", nil)
}
