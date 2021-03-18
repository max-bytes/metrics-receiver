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
	t2 := time.Now()

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
	// insertQueryMetric := "INSERT INTO metric() VALUES ($1,$2,$3,$4,$5,$6)"
	// insertQueryState := "INSERT INTO state() VALUES ($1,$2,$3,$4)"
	expected := []TimescaleRows{
		{
			InsertColumns: []string{"time", "data", "value", "min", "host", "customer"},
			InsertRows: [][]interface{}{
				{
					t1,
					edMetric1,
					"value_value", nil,
					"host_value", nil,
				}, {
					t2,
					edMetric2,
					"value_value", nil,
					"host_value", nil,
				},
			},
			TargetTable: "metric",
		},
		{
			InsertColumns: []string{"time", "data", "warn", "monitoringprofile"},
			InsertRows: [][]interface{}{
				{
					t2,
					edState1,
					"warn_value",
					nil,
				}, {
					t2,
					edState2,
					nil,
					"monitoringprofile_value",
				},
			},
			TargetTable: "state",
		},
	}
	assert.Equal(t, expected, rows)
}

func BenchmarkBuildDBRowsTimescale(b *testing.B) {

	t1 := time.Now()
	t2 := time.Now()

	numMetrics := 1000
	numStates := 1000
	metricPointsCandidates := []general.Point{
		{Measurement: "metric", Fields: map[string]interface{}{"value": "value_value", "warn": "warn_value"}, Tags: map[string]string{"host": "host_value"}, Timestamp: t1},
		{Measurement: "metric", Fields: map[string]interface{}{"value": "value_value", "crit": "crit_value"}, Tags: map[string]string{"host": "host_value", "service": "service_value"}, Timestamp: t2},
	}
	statePointsCandidates := []general.Point{
		{Measurement: "state", Fields: map[string]interface{}{"warn": "warn_value"}, Tags: map[string]string{"host": "host_value"}, Timestamp: t2},
		{Measurement: "state", Fields: map[string]interface{}{"crit": "crit_value"}, Tags: map[string]string{"monitoringprofile": "monitoringprofile_value"}, Timestamp: t2},
	}
	metricPoints := make([]general.Point, 0)
	for i := 0; i < numMetrics; i++ {
		index := i % len(metricPointsCandidates)
		metricPoints = append(metricPoints, metricPointsCandidates[index])
	}
	statePoints := make([]general.Point, 0)
	for i := 0; i < numStates; i++ {
		index := i % len(statePointsCandidates)
		statePoints = append(statePoints, statePointsCandidates[index])
	}

	pointGroups := []general.PointGroup{
		{Measurement: "metric", Points: metricPoints},
		{Measurement: "state", Points: statePoints},
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

	b.ResetTimer()

	// for i := 0; i < 100; i++ {
	_, err := buildDBRowsTimescale(pointGroups, &config)
	b.Log(err)
	// }
}
