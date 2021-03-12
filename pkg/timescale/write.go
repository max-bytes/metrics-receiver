package timescale

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
)

func Write(groupedPoints []general.PointGroup, config *config.OutputTimescale) error {
	var rows, buildDBRowsErr = buildDBRowsTimescale(groupedPoints, config)

	if buildDBRowsErr != nil {
		return fmt.Errorf("An error ocurred while building db rows: %w", buildDBRowsErr)
	}

	if len(rows) > 0 {
		insertErr := insertRowsTimescale(rows, config)
		if insertErr != nil {
			return fmt.Errorf("An error ocurred while inserting db rows: %w", insertErr)
		}
	}
	return nil
}

func buildDBRowsTimescale(i []general.PointGroup, config *config.OutputTimescale) ([]TimescaleRows, error) {
	var rows []TimescaleRows
	for _, input := range i {
		var points = input.Points
		var measurement = input.Measurement

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

		var insertRows []TimescaleRow

		points = general.FilterPoints(points, config)

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

			item := TimescaleRow{
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

		rows = append(rows, TimescaleRows{sql, insertRows, targetTable})
	}

	return rows, nil
}

func insertRowsTimescale(rows []TimescaleRows, config *config.OutputTimescale) error {
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
	InsertQuery string
	InsertRows  []TimescaleRow
	TargetTable string
}

type TimescaleRow struct {
	timestampFormatted string
	encodedData        []byte
	fieldColumnValues  []interface{}
	tagColumnValues    []interface{}
}
