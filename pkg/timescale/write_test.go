package timescale

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
)

func TestBuildDBRowsTimescale(t *testing.T) {

	t1 := time.Now()
	t1f := t1.Format("2006-01-02 15:04:05.000 MST")
	t2 := time.Now()
	t2f := t2.Format("2006-01-02 15:04:05.000 MST")

	pointGroups := []general.PointGroup{
		{Measurement: "metric", Points: []general.Point{
			{Measurement: "metric", Fields: map[string]interface{}{"value": "value_value", "warn": "warn_value"}, Tags: map[string]string{"host": "host_value"}, Timestamp: t1},
			{Measurement: "metric", Fields: map[string]interface{}{"value": "value_value", "crit": "crit_value"}, Tags: map[string]string{"host": "host_value", "service": "service_value"}, Timestamp: t2},
		}},
		{Measurement: "state", Points: []general.Point{
			{Measurement: "state", Fields: map[string]interface{}{"warn": "warn_value"}, Tags: map[string]string{"host": "host_value"}, Timestamp: t2},
			{Measurement: "state", Fields: map[string]interface{}{"crit": "crit_value"}, Tags: map[string]string{"monitoringprofile": "monitoringprofile_value"}, Timestamp: t2},
		}},
		{Measurement: "invalidMeasurement", Points: []general.Point{
			{Measurement: "invalidMeasurement", Fields: map[string]interface{}{"warn": "warn_value"}, Tags: map[string]string{"host": "host_value"}, Timestamp: t2},
			{Measurement: "invalidMeasurement", Fields: map[string]interface{}{"crit": "crit_value"}, Tags: map[string]string{"monitoringprofile": "monitoringprofile_value"}, Timestamp: t2},
		}},
	}
	config := config.OutputTimescale{
		Measurements: map[string]config.MeasurementTimescale{
			"metric": {
				AddedTags:       map[string]string{"added_tag": "added_tag_value"},
				FieldsAsColumns: []string{"value", "min"},
				TagsAsColumns:   []string{"host", "customer"},
				TargetTable:     "metric",
			},
			"state": {
				AddedTags:       map[string]string{"added_tag": "added_tag_value"},
				FieldsAsColumns: []string{"warn"},
				TagsAsColumns:   []string{"monitoringprofile"},
				TargetTable:     "state",
			},
			"invalidMeasurement": {Ignore: true},
		},
	}
	rows, err := buildDBRowsTimescale(pointGroups, &config)
	assert.Nil(t, err)

	edMetric1, _ := json.Marshal(map[string]interface{}{"warn": "warn_value", "added_tag": "added_tag_value"})
	edMetric2, _ := json.Marshal(map[string]interface{}{"crit": "crit_value", "service": "service_value", "added_tag": "added_tag_value"})
	edState1, _ := json.Marshal(map[string]interface{}{"added_tag": "added_tag_value", "host": "host_value"})
	edState2, _ := json.Marshal(map[string]interface{}{"crit": "crit_value", "added_tag": "added_tag_value"})
	insertQueryMetric := "INSERT INTO metric(time,data,value,min,host,customer) VALUES ($1,$2,$3,$4,$5,$6)"
	insertQueryState := "INSERT INTO state(time,data,warn,monitoringprofile) VALUES ($1,$2,$3,$4)"
	expected := []TimescaleRows{
		{
			InsertQuery: insertQueryMetric,
			InsertRows: []TimescaleRow{
				{
					timestampFormatted: t1f,
					encodedData:        edMetric1,
					fieldColumnValues:  []interface{}{"value_value", nil},
					tagColumnValues:    []interface{}{"host_value", nil},
				}, {
					timestampFormatted: t2f,
					encodedData:        edMetric2,
					fieldColumnValues:  []interface{}{"value_value", nil},
					tagColumnValues:    []interface{}{"host_value", nil},
				},
			},
			TargetTable: "metric",
		},
		{
			InsertQuery: insertQueryState,
			InsertRows: []TimescaleRow{
				{
					timestampFormatted: t1f,
					encodedData:        edState1,
					fieldColumnValues:  []interface{}{"warn_value"},
					tagColumnValues:    []interface{}{nil},
				}, {
					timestampFormatted: t2f,
					encodedData:        edState2,
					fieldColumnValues:  []interface{}{nil},
					tagColumnValues:    []interface{}{"monitoringprofile_value"},
				},
			},
			TargetTable: "state",
		},
	}
	assert.Equal(t, expected, rows)
}
