package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
)

func main() {

	http.HandleFunc("/influx/v1/write", influxWriteHandler)
	http.HandleFunc("/influx/v1/query", influxWriteHandler)

	fmt.Printf("Starting server at port 8080\n")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

// POST /influx/v1/write

func influxWriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/influx/v1/write" {
		http.Error(w, "404 not found.", http.StatusNotFound)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusNotFound)
		return
	}

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal("request", err)
	}
	// fmt.Println(string(buf))

	requestStr := string(buf)

	// t := []string{
	// 	"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
	// 	"weather,location=us-midwest temperature=82",                     // no timestamp
	// 	"weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"}

	// influx.Parse(strings.Join(t, "\n"))
	res, _ := influx.Parse(requestStr)
	resJson, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	//Set Content-Type header so that clients will know how to read response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	//Write json response back to response
	w.Write(resJson)
}

// POST /influx/v1/query

func influxQueryHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/influx/v1/query" {
		http.Error(w, "404 not found.", http.StatusNotFound)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusNotFound)
		return
	}

	http.Error(w, "Not authorized", 401)
	return
}
