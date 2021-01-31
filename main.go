package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	flag "github.com/spf13/pflag"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
)

var (
	version    = "0.0.0-src"
	configFile = flag.String("config", "config.json", "Config file location")
)

var config Configuration

func main() {

	flag.Parse()
	file, err := os.Open(*configFile)
	if err != nil {
		log.Fatal("can't open config file: ", err)
	}
	defer file.Close()

	// var Config Configuration

	byteValue, _ := ioutil.ReadAll(file)
	json.Unmarshal(byteValue, &config)
	// log.Println(config.TimescaleConnectionString)

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

	var a = measurementSplitter(res)
	fmt.Println(a)

	var _, _, e = buildWriteFlow(a, config)
	fmt.Println(e)

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

func measurementSplitter(input []influx.Point) []Ret {

	// var groupedPoints []influx.Point

	/*
	   $points = $input->getPoints();

	   $groupedPoints = [];
	   foreach($points as $point) {
	       $measurement = $point->getMeasurement();
	       $groupedPoints[$measurement][] = $point;
	   }

	   $ret = [];
	   foreach($groupedPoints as $measurement => $points) {
	       $ret[] = [
	           'measurement' => $measurement,
	           'points' => $points
	       ];
	   }

	   return new \ArrayObject($ret);
	*/

	var groupedPoints map[string][]influx.Point = make(map[string][]influx.Point)

	for _, point := range input {
		var measurement = point.Measurement
		groupedPoints[measurement] = append(groupedPoints[measurement], point)
	}

	var r []Ret

	for measurement, points := range groupedPoints {
		var p = Ret{measurement, points}
		r = append(r, p)
	}

	return r
}

func buildWriteFlow(i []Ret, config Configuration) (interface{}, interface{}, error) {
	for _, input := range i {

		var points = input.points
		var measurement = input.measurement

		if _, ok := config.Measurements[0][measurement]; ok == false {
			return nil, nil, errors.New("Unknown measurement \"{$measurement}\" encountered")
		}

		var measurementConfig = config.Measurements[0][measurement]

		if _, ok := measurementConfig[0]["ignore"]; ok {
			return nil, nil, nil
		}

		var tagsAsColumns = measurementConfig[0]["tagsAsColumns"]
		var fieldsAsColumns = measurementConfig[0]["fieldsAsColumns"]

		var addedTags []interface{} = nil

		if _, ok := measurementConfig[0]["addedTags"]; ok {
			addedTags = measurementConfig[0]["addedTags"]
		}

		for _, point := range points {

			var timestamp time.Time
			if point.Timestamp != "" {
				t, _ := strconv.Atoi(point.Timestamp)
				timestamp = time.Unix(0, int64(t))
			} else {
				timestamp = time.Now()
			}

			var timestampFormatted = timestamp.Format("2006-01-02 03:04:05.000 	MST")
			fmt.Println(timestampFormatted)

			var tags = point.Tags

			if addedTags != nil {
				// merge these two maps
				for _, v := range addedTags {
					switch v.(type) {
					case map[string]interface{}:
						for key, value := range v.(map[string]interface{}) {
							tags[key] = value.(string)
						}
					default:
						fmt.Printf("%v is unknown \n ", v)
					}
				}
			}
			var tagColumnValues []string

			for _, v := range tagsAsColumns {
				if _, ok := tags[v.(string)]; ok {
					tagColumnValues = append(tagColumnValues, v.(string))
				}
			}

			var tagDataValues []interface{}

			for key := range tags {
				for _, v := range tagsAsColumns {
					if key == v.(string) {
						tagDataValues = append(tagDataValues, tags[v.(string)])
					}
				}
			}
			var fields = point.Fields
			var fieldColumnValues []string

			for _, v := range fieldsAsColumns {
				if _, ok := fields[v.(string)]; ok {
					fieldColumnValues = append(fieldColumnValues, v.(string))
				}
			}

			var fieldDataValues []interface{}

			for key := range fields {
				for _, v := range fieldsAsColumns {
					if key == v.(string) {
						fieldDataValues = append(fieldDataValues, fields[v.(string)])
					}
				}
			}

			// return array_merge(
			// 	[$timestampFormatted],
			// 	[json_encode(array_merge($tagDataValues, $fieldDataValues))],
			// 	$fieldColumnValues,
			// 	$tagColumnValues
			// );
		}

		db, err := sql.Open("postgres", config.TimescaleConnectionString)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		var baseColumns []interface{} = []interface{}{"time", "data"}
		targetTable := measurementConfig[0]["targetTable"]

		allColumns := ArrayMerge(baseColumns, fieldsAsColumns, tagsAsColumns)

		var c []string

		for _, value := range allColumns {
			switch value.(type) {
			case string:
				c = append(c, value.(string))
			default:
				fmt.Printf("%v is unknown \n ", value)
			}
		}

		columnsSQLStr := strings.Join(c, ",")

		var a []string = MakeRange(1, len(allColumns))

		var placeholdersSQLStr = strings.Join(a, ",")

		sql := fmt.Sprintf("INSERT INTO %v(%v) VALUES (%v)", targetTable[0].(string), columnsSQLStr, placeholdersSQLStr)
		fmt.Println(sql)
		// $baseColumns = ['time', 'data'];
		// $targetTable = $measurementConfig['targetTable'];
		// $allColumns = array_merge($baseColumns, $fieldsAsColumns, $tagsAsColumns);
		// $columnsSQLStr = implode(',', array_map(function($c) { return "\"$c\""; }, $allColumns));
		// $placeholdersSQLStr = implode(',', array_map(function($index) { return "\${$index}"; }, range(1, sizeof($allColumns))));
		// $sql = "INSERT INTO $targetTable($columnsSQLStr) VALUES ($placeholdersSQLStr)";
	}

	return nil, nil, nil
}

// Structs to parse influx data

// type Series struct {
// 	Points []influx.Point
// }

type Ret struct {
	measurement string
	points      []influx.Point
}

// Structs used to parse configuration
type Configuration struct {
	TimescaleConnectionString string `json:"timescaleConnectionString"`
	// Measurements              []Measurement `json:"measurements"`
	Measurements []map[string][]map[string][]interface{} `json:"measurements"`
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

func ArrayMerge(ss ...[]interface{}) []interface{} {
	n := 0
	for _, v := range ss {
		n += len(v)
	}
	s := make([]interface{}, 0, n)
	for _, v := range ss {
		s = append(s, v...)
	}
	return s
}

func MakeRange(min, max int) []string {
	a := make([]string, max-min+1)
	for i := range a {
		a[i] = strconv.Itoa(min + i)
	}
	return a
}

func In_array(needle interface{}, hystack interface{}) bool {
	switch key := needle.(type) {
	case string:
		for _, item := range hystack.([]string) {
			if key == item {
				return true
			}
		}
	case int:
		for _, item := range hystack.([]int) {
			if key == item {
				return true
			}
		}
	case int64:
		for _, item := range hystack.([]int64) {
			if key == item {
				return true
			}
		}
	default:
		return false
	}
	return false
}
