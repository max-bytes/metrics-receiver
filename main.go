package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"
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

	byteValue, _ := ioutil.ReadAll(file)
	json.Unmarshal(byteValue, &config)

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
		log.Fatal("request", err)
	}

	requestStr := string(buf)

	res, _ := influx.Parse(requestStr)

	var a = measurementSplitter(res)

	var sql, insertedRows, write_err = buildWriteFlow(a, config)

	if write_err != nil {
		http.Error(w, "Error on parsing the provieded file!", http.StatusBadRequest)
		return
	}

	insert_error := insertRows(sql, insertedRows, config)

	if insert_error != nil {
		http.Error(w, insert_error.Error(), http.StatusBadRequest)
		return
	}

	//Set Content-Type header so that clients will know how to read response
	// w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	//Write json response back to response
	// w.Write(resJson)
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

func buildWriteFlow(i []Ret, config Configuration) (string, []InsertRow, error) {
	for _, input := range i {

		var points = input.points
		var measurement = input.measurement

		if _, ok := config.Measurements[measurement]; ok == false {
			return "", nil, errors.New("Unknown measurement \"{$measurement}\" encountered")
		}

		var measurementConfig = config.Measurements[measurement]

		if _, ok := measurementConfig["ignore"]; ok {
			return "", nil, nil
		}

		var tagsAsColumns = measurementConfig["tagsAsColumns"]
		var fieldsAsColumns = measurementConfig["fieldsAsColumns"]

		var addedTags []interface{} = nil

		if _, ok := measurementConfig["addedTags"]; ok {
			addedTags = measurementConfig["addedTags"]
		}

		var insertRows []InsertRow

		for _, point := range points {

			var timestamp time.Time
			if point.Timestamp != "" {
				t, _ := strconv.Atoi(point.Timestamp)
				timestamp = time.Unix(0, int64(t))
			} else {
				timestamp = time.Now()
			}

			var timestampFormatted = timestamp.Format("2006-01-02 03:04:05.000 MST")

			var tags = point.Tags

			if addedTags != nil {
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
			var tagColumnValues []interface{}

			for _, v := range tagsAsColumns {
				if _, ok := tags[v.(string)]; ok {
					tagColumnValues = append(tagColumnValues, tags[v.(string)])
				}
			}

			var tagDataValues map[string]interface{} = make(map[string]interface{})

			for key, tagValue := range tags {
				for _, v := range tagsAsColumns {
					if key == v.(string) {
						tagDataValues[key] = tagValue
					}
				}
			}

			var fields = point.Fields
			var fieldColumnValues []interface{}

			for _, v := range fieldsAsColumns {
				if _, ok := fields[v.(string)]; ok {
					fieldColumnValues = append(fieldColumnValues, fields[v.(string)])
				}
			}

			var fieldDataValues map[string]interface{} = make(map[string]interface{})

			for key, fieldValue := range fields {
				for _, v := range fieldsAsColumns {
					if key == v.(string) {
						fieldDataValues[key] = fieldValue
					}
				}
			}

			encodedData, _ := json.Marshal(MapsMerge(tagDataValues, fieldDataValues))

			item := InsertRow{
				timestampFormatted: timestampFormatted,
				encodedData:        encodedData,
				fieldColumnValues:  fieldColumnValues,
				tagColumnValues:    tagColumnValues,
			}

			insertRows = append(insertRows, item)
		}

		var baseColumns []interface{} = []interface{}{"time", "data"}
		targetTable := measurementConfig["targetTable"]

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

		return sql, insertRows, nil
	}

	return "", nil, nil
}

func insertRows(insertQuery string, transformedInput []InsertRow, config Configuration) error {

	var c, parseErr = pgx.ParseConnectionString(config.TimescaleConnectionString)
	if parseErr != nil {
		log.Fatal(parseErr)
		return parseErr
	}

	conn, connErr := pgx.Connect(c)
	if connErr != nil {
		log.Fatal(connErr)
		return connErr
	}
	defer conn.Close()

	tx, beginErr := conn.Begin()

	if beginErr != nil {
		log.Fatal(beginErr)
		return beginErr
	}
	defer tx.Rollback()

	stmt, prepareErr := tx.Prepare("insert_query", insertQuery)
	if prepareErr != nil {
		log.Fatal(prepareErr)
		return prepareErr
	}
	defer tx.Rollback()

	for _, ti := range transformedInput {

		var a []interface{}

		a = append(a, ti.timestampFormatted)
		a = append(a, ti.encodedData)
		for _, val := range ti.fieldColumnValues {
			a = append(a, val)
		}
		for _, val := range ti.tagColumnValues {
			a = append(a, val)
		}

		_, insertErr := tx.Exec(stmt.SQL, a...)
		if insertErr != nil {
			log.Fatal(insertErr)
			return insertErr
		}
	}

	err := tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

type Ret struct {
	measurement string
	points      []influx.Point
}

type InsertRow struct {
	timestampFormatted string
	encodedData        []byte
	fieldColumnValues  []interface{}
	tagColumnValues    []interface{}
}

// Structs used to parse configuration
type Configuration struct {
	TimescaleConnectionString string                              `json:"timescaleConnectionString"`
	Measurements              map[string]map[string][]interface{} `json:"measurements"`
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

func MapsMerge(ss ...map[string]interface{}) map[string]interface{} {
	s := make(map[string]interface{})
	for _, item := range ss {
		for key, value := range item {
			s[key] = value
		}
	}
	return s
}

func MakeRange(min, max int) []string {
	a := make([]string, max-min+1)
	for i := range a {
		a[i] = "$" + strconv.Itoa(min+i)
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
