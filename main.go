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

	requestStr := string(buf)

	res, _ := influx.Parse(requestStr)
	resJson, err := json.Marshal(res)
	if err != nil {
		http.Error(w, "Error on parsing the provieded file!", http.StatusNotFound)
		return
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
