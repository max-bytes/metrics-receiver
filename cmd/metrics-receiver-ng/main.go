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

	byteValue, readErr := ioutil.ReadAll(file)
	if readErr != nil {
		log.Fatal("can't read config file: ", err)
	}
	json.Unmarshal(byteValue, &config)

	http.HandleFunc("/api/influx/v1/write", influxWriteHandler)
	http.HandleFunc("/api/influx/v1/query", influxQueryHandler)

	fmt.Printf("Starting server at port 80\n")
	if err := http.ListenAndServe(":80", nil); err != nil {
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

	res, parseErr := influx.Parse(requestStr)
	if parseErr != nil {
		log.Println(parseErr)
		http.Error(w, "An error ocurred while parsing the provieded file!", http.StatusBadRequest)
		return
	}

	var splittedRows = measurementSplitter(res)

	var rows, buildDBRowsErr = buildDBRows(splittedRows, config)

	if buildDBRowsErr != nil {
		log.Println(buildDBRowsErr)
		http.Error(w, "An error ocurred while building db rows!", http.StatusBadRequest)
		return
	}

	insertErr := insertRows(rows, config)

	if insertErr != nil {
		log.Println(insertErr)
		http.Error(w, insertErr.Error(), http.StatusBadRequest)
		return
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

func buildDBRows(i []Ret, config Configuration) ([]DBRow, error) {
	var rows []DBRow

	for _, input := range i {
		var points = input.points
		var measurement = input.measurement

		if _, ok := config.Measurements[measurement]; ok == false {
			return nil, errors.New("Unknown measurement \"{$measurement}\" encountered")
		}

		var measurementConfig = config.Measurements[measurement]

		if measurementConfig.Ignore {
			return nil, nil
		}

		var tagsAsColumns = measurementConfig.TagsAsColumns
		var fieldsAsColumns = measurementConfig.FieldsAsColumns

		var addedTags map[string]string = nil

		if measurementConfig.AddedTags != nil {
			addedTags = measurementConfig.AddedTags
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
				for k, v := range addedTags {
					tags[k] = v
				}
			}
			var tagColumnValues []interface{}

			for _, v := range tagsAsColumns {
				if _, ok := tags[v]; ok {
					tagColumnValues = append(tagColumnValues, tags[v])
				} else {
					tagColumnValues = append(tagColumnValues, nil)
				}
			}

			var tagDataValues map[string]interface{} = make(map[string]interface{})

			for key, tagValue := range tags {
				for _, v := range tagsAsColumns {
					if key == v {
						tagDataValues[key] = tagValue
					}
				}
			}

			var fields = point.Fields
			var fieldColumnValues []interface{}

			for _, v := range fieldsAsColumns {
				if _, ok := fields[v]; ok {
					fieldColumnValues = append(fieldColumnValues, fields[v])
				} else {
					fieldColumnValues = append(fieldColumnValues, nil)
				}
			}

			var fieldDataValues map[string]interface{} = make(map[string]interface{})

			for key, fieldValue := range fields {
				for _, v := range fieldsAsColumns {
					if key == v {
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

		var baseColumns []string = []string{"time", "data"}
		targetTable := measurementConfig.TargetTable

		allColumns := ArrayMerge(baseColumns, fieldsAsColumns, tagsAsColumns)

		var c []string

		for _, value := range allColumns {
			c = append(c, value)
		}

		columnsSQLStr := strings.Join(c, ",")

		var a []string = CreateBindParameterList(1, len(allColumns))

		var placeholdersSQLStr = strings.Join(a, ",")

		sql := fmt.Sprintf("INSERT INTO %v(%v) VALUES (%v)", targetTable, columnsSQLStr, placeholdersSQLStr)

		rows = append(rows, DBRow{sql, insertRows})
	}

	return rows, nil
}

func insertRows(rows []DBRow, config Configuration) error {
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
		log.Println(beginErr)
		return beginErr
	}

	for _, row := range rows {

		stmt, prepareErr := tx.Prepare("insert_query", row.InsertQuery)
		if prepareErr != nil {
			log.Println(prepareErr)
			tx.Rollback()
			return prepareErr
		}
		for _, ti := range row.InsertRows {

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
				log.Println(insertErr)
				tx.Rollback()
				return insertErr
			}
		}
	}

	err := tx.Commit()
	if err != nil {
		log.Println(err)
		tx.Rollback()
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

type DBRow struct {
	InsertQuery string
	InsertRows  []InsertRow
}

type Configuration struct {
	TimescaleConnectionString string                              `json:"timescaleConnectionString"`
	Measurements              map[string]MeasurementConfiguration `json:"measurements"`
}

type MeasurementConfiguration struct {
	AddedTags       map[string]string
	FieldsAsColumns []string
	TagsAsColumns   []string
	TargetTable     string
	Ignore          bool
}

func ArrayMerge(ss ...[]string) []string {
	n := 0
	for _, v := range ss {
		n += len(v)
	}
	s := make([]string, 0, n)
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

func CreateBindParameterList(min, max int) []string {
	a := make([]string, max-min+1)
	for i := range a {
		a[i] = "$" + strconv.Itoa(min+i)
	}
	return a
}
