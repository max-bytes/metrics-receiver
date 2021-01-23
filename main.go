package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	flag "github.com/spf13/pflag"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
)

var (
	version    = "0.0.0-src"
	configFile = flag.String("config", "config.json", "Config file location")
)

func main() {

	flag.Parse()
	file, err := os.Open(*configFile)
	if err != nil {
		log.Fatal("can't open config file: ", err)
	}
	defer file.Close()

	var Config Configuration

	byteValue, _ := ioutil.ReadAll(file)
	json.Unmarshal(byteValue, &Config)
	log.Println(Config.TimescaleConnectionString)

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

type Configuration struct {
	TimescaleConnectionString string        `json:"timescaleConnectionString"`
	Measurements              []Measurement `json:"measurements"`
}

type Measurement struct {
	Value            []Value            `json:"value"`
	RabbitmqExchange []RabbitmqExchange `json:"rabbitmq_exchange"`
	RabbitmqQueue    []RabbitmqQueue    `json:"rabbitmq_queue"`
	RabbitmqNode     []RabbitmqNode     `json:"rabbitmq_node"`
}

type Value struct {
	FieldsAsColumns []string `json:"fieldsAsColumns"`
	TagsAsColumns   []string `json:"tagsAsColumns"`
	TargetTable     string   `json:"targetTable"`
}

type RabbitmqExchange struct {
	AddedTags       []AddedTags `json:"addedTags"`
	FieldsAsColumns []string    `json:"fieldsAsColumns"`
	TagsAsColumns   []string    `json:"tagsAsColumns"`
	TargetTable     string      `json:"targetTable"`
}

type RabbitmqQueue struct {
	AddedTags       []AddedTags `json:"addedTags"`
	FieldsAsColumns []string    `json:"fieldsAsColumns"`
	TagsAsColumns   []string    `json:"tagsAsColumns"`
	TargetTable     string      `json:"targetTable"`
}

type RabbitmqNode struct {
	Ignore bool `json:"addedTags"`
}

type AddedTags struct {
	Measurement string `json:"measurement"`
}

// json config file
// "measurements": [
//     {
//         "value": [
//             {
//                 "fieldsAsColumns": ["value", "warn", "crit", "min", "max"],
//                 "tagsAsColumns": ["host", "service", "label", "uom"],
//                 "targetTable": "metric"
//             }
//         ],
//         "rabbitmq_exchange": [
//             {
//                 "addedTags": [
//                     {
//                         "measurement": "exchange"
//                     }
//                 ],
//                 "fieldsAsColumns": [],
//                 "tagsAsColumns": [],
//                 "targetTable": "rabbitmq"
//             }
//         ],
//         "rabbitmq_queue": [
//             {
//                 "addedTags": [
//                     {
//                         "measurement": "queue"
//                     }
//                 ],
//                 "fieldsAsColumns": [],
//                 "tagsAsColumns": [],
//                 "targetTable": "rabbitmq"
//             }
//         ],
//         "rabbitmq_node": [
//             {
//                 "ignore": true
//             }
//         ]
//     }
// ]
