package main

import (
	"C"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

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

func deposit_keywords(kw string, id string) {

	retries := 0
	for retries < 5 {
		data, err := gremlin.Query(`g.V(g.V().has("keyword", "content", \
			_keyword).tryNext().orElseGet({return g.addV(label, "keyword", "content", \
				_keyword).next()})).addE('occurs_in').to(g.V(_article_id))`).Bindings(
			gremlin.Bind{"_keyword": kw, "_article_id": id}).Exec()
		// fmt.Println(string(data))
		retries += 1
		if err != nil {
			if retries > 3 {
				fmt.Println(kw)
			}
			fmt.Println("In deposit_keywords")
			if err := gremlin.NewCluster("ws://localhost:8182", "ws://staging.local:8182"); err != nil {
				// handle error
			}
			fmt.Println(err)
			fmt.Println(string(data))
			fmt.Println(retries)
			time.Sleep(1000 * time.Duration(math.Exp2(float64(retries))) * time.Millisecond)
		} else {
			break
		}

	}

}

//export deposit_article
func deposit_article(articleJSON *C.char) *C.char {
	concurrency := 10
	sem := make(chan bool, concurrency)
	if err := gremlin.NewCluster("ws://localhost:8182", "ws://staging.local:8182"); err != nil {
		// handle error
	}
	var article Article
	rawstring := C.GoString(articleJSON)
	var articleString = []byte(rawstring)
	err := json.Unmarshal(articleString, &article)
	data, err := gremlin.Query(`g.V().has("article", "pmid", _pmid).has(\
		"pmc", _pmc).has("doi", _doi).tryNext().orElse(g.addV(label, "article",\
			"pmid", _pmid, "full_title", _full_title, "pmc", _pmc, "doi", _doi,\
			"publication_year",_publication_year)).id()`).Bindings(
		gremlin.Bind{"_pmid": article.Pmid, "_full_title": article.Title,
			"_pmc": article.Pmc, "_doi": article.Doi,
			"_publication_year": article.Publication_year}).Exec()

	if err != nil {
		fmt.Println("in deposit_article")
		fmt.Println(err)
		fmt.Println(string(data))
	}

	fmt.Println("Deposited article")

	for _, kw := range article.Keywords {
		// fmt.Println(kw)
		sem <- true
		go func(kw string, id string) {
			defer func() { <-sem }()
			deposit_keywords(kw, id)
		}(kw, strings.Replace(strings.Replace(string(data), "[", "", 1), "]", "", 1))
		// go deposit_keywords(kw, strings.Replace(strings.Replace(string(data), "[", "", 1), "]", "", 1))
		// fmt.Println(string(data))
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	return C.CString(string(data))
}

func main() {
}
