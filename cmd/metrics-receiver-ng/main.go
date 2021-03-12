package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/jackc/pgx"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
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
	for _, output := range cfg.OutputsTimescale {
		var rows, buildDBRowsErr = buildDBRowsTimescale(splittedRows, output)

		if buildDBRowsErr != nil {
			log.Println(buildDBRowsErr)
			if output.WriteStrategy == "commit" {
				http.Error(w, "An error ocurred while building db rows!", http.StatusBadRequest)
				return
			}
		}

		if len(rows) > 0 {
			insertErr := insertRowsTimescale(rows, output)

			if insertErr != nil {
				log.Println(insertErr)
				if output.WriteStrategy == "commit" {
					http.Error(w, insertErr.Error(), http.StatusBadRequest)
					return
				}
			}

		}
	}

	// influxdb outputs
	for _, output := range cfg.OutputsInflux {
		var points, err = buildDBPointsInflux(splittedRows, output)

		if err != nil {
			log.Println(err)
			if output.WriteStrategy == "commit" {
				http.Error(w, "An error ocurred while building db rows!", http.StatusBadRequest)
				return
			}
		}

		if len(points) > 0 {
			insertErr := insertRowsInflux(points, output)
			if output.WriteStrategy == "commit" {
				if insertErr != nil {
					log.Println(insertErr)
					http.Error(w, insertErr.Error(), http.StatusBadRequest)
					return
				}
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

func buildDBRowsTimescale(i []Ret, config config.OutputTimescale) ([]DBRow, error) {
	var rows []DBRow
	for _, input := range i {
		var points = input.points
		var measurement = input.measurement

		if _, ok := config.Measurements[measurement]; ok == false {
			return nil, errors.New(fmt.Sprintf("Unknown measurement \"%s\" encountered", measurement))
		}

		var measurementConfig = config.Measurements[measurement]

		if measurementConfig.Ignore {
			continue
		}

		var tagsAsColumns = measurementConfig.TagsAsColumns
		var fieldsAsColumns = measurementConfig.FieldsAsColumns

		var addedTags map[string]string = nil

		if measurementConfig.AddedTags != nil {
			addedTags = measurementConfig.AddedTags
		}

		var insertRows []InsertRow

		points = filterPoints(points, config)

		for _, point := range points {
			var timestampFormatted = point.Timestamp.Format("2006-01-02 03:04:05.000 MST")

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

		rows = append(rows, DBRow{sql, insertRows, targetTable})
	}

	return rows, nil
}

func buildDBPointsInflux(i []Ret, config config.OutputInflux) ([]influx.Point, error) {
	var writePoints []influx.Point
	for _, input := range i {
		var points = input.points
		var measurement = input.measurement

		if _, ok := config.Measurements[measurement]; ok == false {
			return nil, errors.New(fmt.Sprintf("Unknown measurement \"%s\" encountered", measurement))
		}

		var measurementConfig = config.Measurements[measurement]

		if measurementConfig.Ignore {
			continue
		}

		var addedTags map[string]string = nil

		if measurementConfig.AddedTags != nil {
			addedTags = measurementConfig.AddedTags
		}

		points = filterPoints(points, config)

		for _, point := range points {

			var tags = point.Tags

			if addedTags != nil {
				for k, v := range addedTags {
					tags[k] = v
				}
			}

			writePoints = append(writePoints, influx.Point{
				Measurement: measurement,
				Fields:      point.Fields,
				Tags:        tags,
				Timestamp:   point.Timestamp})

		}
	}

	return writePoints, nil
}

func insertRowsInflux(writePoints []influx.Point, config config.OutputInflux) error {

	// create new client with default option for server url authenticate by token
	client := influxdb2.NewClient(config.Connection, config.AuthToken)

	// user blocking write client for writes to desired bucket
	writeAPI := client.WriteAPIBlocking(config.Org, config.DbName)

	for _, p := range writePoints {
		p1 := influxdb2.NewPoint(p.Measurement,
			p.Tags,
			p.Fields,
			p.Timestamp)

		err := writeAPI.WritePoint(context.Background(), p1)
		if err != nil {
			return err
		}
	}

	// Ensures background processes finish
	client.Close()
	return nil
}

func insertRowsTimescale(rows []DBRow, config config.OutputTimescale) error {
	var c, parseErr = pgx.ParseConnectionString(config.Connection)
	if parseErr != nil {
		return parseErr
	}

	conn, connErr := pgx.Connect(c)
	if connErr != nil {
		return connErr
	}
	defer conn.Close()

	tx, beginErr := conn.Begin()
	defer tx.Rollback()

	if beginErr != nil {
		return beginErr
	}

	for _, row := range rows {

		stmt, prepareErr := tx.Prepare(fmt.Sprintf("insert_query_%v", row.TargetTable), row.InsertQuery)
		if prepareErr != nil {
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
				return insertErr
			}
		}
	}

	err := tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func filterPoints(points []influx.Point, c interface{}) []influx.Point {

	var tagfilterInclude map[string][]string = make(map[string][]string)
	var tagfilterBlock map[string][]string = make(map[string][]string)
	switch v := c.(type) {
	case config.OutputInflux:
		tagfilterInclude = v.TagfilterInclude
		tagfilterBlock = v.TagfilterBlock
	case config.OutputTimescale:
		tagfilterInclude = v.TagfilterInclude
		tagfilterBlock = v.TagfilterBlock
	default:
		fmt.Printf("I don't know about type %T!\n", v)
	}

	var filteredPoints []influx.Point
	if len(tagfilterInclude) == 0 {
		// no filtering
		filteredPoints = points
	} else {
		for _, point := range points {
		outInclude:
			for tagKey, tagValue := range point.Tags {
				if _, ok := tagfilterInclude[tagKey]; ok == true {
					for _, v := range tagfilterInclude[tagKey] {
						if v == "*" || tagValue == v {
							filteredPoints = append(filteredPoints, point)
							break outInclude
						}
					}
				}
			}
		}
	}

	var keysToDelete []int
	for pointKey, point := range filteredPoints {

	outBlock:
		for tagKey, tagValue := range point.Tags {
			if _, ok := tagfilterBlock[tagKey]; ok == true {
				for _, v := range tagfilterBlock[tagKey] {
					if v == "*" || tagValue == v {
						// index of the value to remove from points array
						keysToDelete = append(keysToDelete, pointKey)
						break outBlock
					}
				}
			}
		}
	}

	var result []influx.Point
	for pointKey := range filteredPoints {
		if Contains(keysToDelete, pointKey) == false {
			result = append(result, filteredPoints[pointKey])
		}
	}

	return result
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
	TargetTable string
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

func Contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
