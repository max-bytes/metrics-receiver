package timescale

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/max-bytes/metrics-receiver/pkg/config"
	"github.com/max-bytes/metrics-receiver/pkg/general"
)

func Write(groupedPoints []general.PointGroup, cfg *config.OutputTimescale, enrichmentSets []config.EnrichmentSet) error {
	var rows, buildDBRowsErr = buildDBRowsTimescale(groupedPoints, cfg, enrichmentSets)

	if buildDBRowsErr != nil {
		return fmt.Errorf("An error ocurred while building db rows: %w", buildDBRowsErr)
	}

	if len(rows) > 0 {
		insertErr := insertRowsTimescale(rows, cfg)
		if insertErr != nil {
			return fmt.Errorf("An error ocurred while inserting rows into timescaleDB: %w", insertErr)
		}
	}
	return nil
}

func buildDBRowsTimescale(i []general.PointGroup, cfg *config.OutputTimescale, enrichmentSets []config.EnrichmentSet) ([]TimescaleRows, error) {
	var rows []TimescaleRows
	for _, input := range i {
		var points = input.Points
		var measurement = input.Measurement

		if _, ok := cfg.Measurements[measurement]; !ok {
			return nil, fmt.Errorf("Unknown measurement \"%s\" encountered", measurement)
		}
		var measurementConfig = cfg.Measurements[measurement]

		var tagsAsColumns = measurementConfig.TagsAsColumns
		var fieldsAsColumns = measurementConfig.FieldsAsColumns

		var insertRows [][]interface{}
		for _, point := range points {

			var tags = point.Tags
			var tagColumnValues []interface{}
			for _, v := range tagsAsColumns {
				if _, ok := tags[v]; ok {
					tagColumnValues = append(tagColumnValues, tags[v])
				} else {
					tagColumnValues = append(tagColumnValues, nil)
				}
			}

			// add tags (that are not mapped to colums) as data values
			var tagDataValues map[string]interface{} = make(map[string]interface{})
			for key, tagValue := range tags {
				if !contains(tagsAsColumns, key) {
					tagDataValues[key] = tagValue
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

			// add fields (that are not mapped to colums) as data values
			var fieldDataValues map[string]interface{} = make(map[string]interface{})
			for key, fieldValue := range fields {
				if !contains(fieldsAsColumns, key) {
					fieldDataValues[key] = fieldValue
				}
			}

			encodedData, _ := json.Marshal(MapsMerge(tagDataValues, fieldDataValues))

			item := []interface{}{
				point.Timestamp,
				encodedData,
			}
			item = append(item, fieldColumnValues...)
			item = append(item, tagColumnValues...)

			insertRows = append(insertRows, item)
		}

		var baseColumns []string = []string{"time", "data"}
		targetTable := measurementConfig.TargetTable

		allColumns := ArrayMerge(baseColumns, fieldsAsColumns, tagsAsColumns)

		rows = append(rows, TimescaleRows{allColumns, insertRows, targetTable})
	}

	return rows, nil
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func insertRowsTimescale(rowsArray []TimescaleRows, config *config.OutputTimescale) error {
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
	if beginErr != nil {
		return beginErr
	}
	defer tx.Rollback() //nolint: errcheck

	for _, rows := range rowsArray {

		copyCount, err := conn.CopyFrom(pgx.Identifier{rows.TargetTable}, rows.InsertColumns, pgx.CopyFromRows(rows.InsertRows))
		if err != nil {
			return fmt.Errorf("Unexpected error for CopyFrom: %v", err)
		}
		if int(copyCount) != len(rows.InsertRows) {
			return fmt.Errorf("Expected CopyFrom to return %d copied rows, but got %d", len(rows.InsertRows), copyCount)
		}
	}

	err := tx.Commit()
	if err != nil {
		return err
	}

	return nil
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

type TimescaleRows struct {
	InsertColumns []string
	InsertRows    [][]interface{}
	TargetTable   string
}
