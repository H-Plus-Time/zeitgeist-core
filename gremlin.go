package main

import (
	"C"
	"encoding/json"
	"fmt"

	"github.com/go-gremlin/gremlin"
)

type Article struct {
	pmid             string
	full_title       string
	pmc              string
	doi              string
	publication_year string
	kwset            []string
}

//export connect
func connect() {
	if err := gremlin.NewCluster("ws://localhost:8182", "ws://staging.local:8182"); err != nil {
		// handle error
	}
}

//export test
func test() *C.char {
	data, err := gremlin.Query(`g.V()`).Exec()
	if err != nil {
		// handle error
		fmt.Println("Please run .connect() first")

	}
	fmt.Println(string("hola"))
	fmt.Println(string(data))
	return C.CString(string(data))
}

//export deposit_article
func deposit_article(articleJSON *C.char) *C.char {
	var article Article
	var articleString = []byte(C.GoString(articleJSON))
	err := json.Unmarshal(articleString, &article)
	data, err := gremlin.Query(`g.addV(label, "article", "pmid", _pmid,\
		"full_title", _full_title, "pmc", _pmc, "doi", _doi,"publication_year",\
		_publication_year)`).Bindings(gremlin.Bind{"_pmid": article.pmid,
		"_full_title": article.full_title, "_pmc": article.pmc, "_doi": article.doi,
		"_publication_year": article.publication_year}).Exec()

	fmt.Println(article.kwset)
	for _, kw := range article.kwset {
		fmt.Println(kw)
	}
	data, err = gremlin.Query(``).Bindings(gremlin.Bind{}).Exec()
	if err != nil {

	}
	return C.CString(string(data))
}

func main() {
}
