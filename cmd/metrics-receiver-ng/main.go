package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/timescale"
)

var (
	version    = "0.0.0-src"
	configFile = flag.String("config", "config.json", "Config file location")
)

var cfg config.Configuration

func main() {

	flag.Parse()

	err := config.ReadConfigFromFile(*configFile, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/api/influx/v1/write", influxWriteHandler)
	http.HandleFunc("/api/influx/v1/query", influxQueryHandler)

	fmt.Printf("Starting server at port %d\n", cfg.Port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil); err != nil {
		log.Fatal(err)
	}
}

// POST /influx/v1/write
func influxWriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	var reader io.ReadCloser
	var err error

	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		defer reader.Close()
	default:
		reader = r.Body
	}

	buf, err := ioutil.ReadAll(reader)

	if err != nil {
		log.Println(err)
		http.Error(w, "An error ocurred while trying to read the request body!", http.StatusBadRequest)
		return
	}

	requestStr := string(buf)

	res, parseErr := influx.Parse(requestStr, time.Now())
	if parseErr != nil {
		log.Println(parseErr)
		http.Error(w, "An error ocurred while parsing the provieded file!", http.StatusBadRequest)
		return
	}

	var splittedRows = measurementSplitter(res)

	// timescaledb outputs
	for _, outputConfig := range cfg.OutputsTimescale {
		err := timescale.Write(splittedRows, outputConfig)

		if err != nil {
			log.Println(err)
			if outputConfig.WriteStrategy == "commit" {
				http.Error(w, "An error occurred handling timescaleDB output: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	// influxdb outputs
	for _, outputConfig := range cfg.OutputsInflux {
		err := influx.Write(splittedRows, outputConfig)

		if err != nil {
			log.Println(err)
			if outputConfig.WriteStrategy == "commit" {
				http.Error(w, "An error occurred handling timescaleDB output: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// GET /influx/v1/query
func influxQueryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	http.Error(w, "Not supported", http.StatusUnauthorized)
	return
}

func measurementSplitter(input []general.Point) []general.PointGroup {

	var groupedPoints map[string][]general.Point = make(map[string][]general.Point)

	for _, point := range input {
		var measurement = point.Measurement
		groupedPoints[measurement] = append(groupedPoints[measurement], point)
	}

	var r []general.PointGroup

	for measurement, points := range groupedPoints {
		var p = general.PointGroup{Measurement: measurement, Points: points}
		r = append(r, p)
	}

	return r
}
