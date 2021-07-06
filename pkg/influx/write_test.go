package influx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
)

func TestBuildPointsInflux(t *testing.T) {

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
	cfg := config.OutputInflux{
		Measurements: map[string]config.MeasurementInflux{
			"metric": {
				AddedTags: map[string]string{"added_tag": "added_tag_value"},
			},
			"state": {
				AddedTags: map[string]string{"added_tag": "added_tag_value"},
			},
			"invalidMeasurement": {Ignore: true},
		},
	}

	preparedPointGroups, err := general.PreparePointGroups(pointGroups, &cfg, []config.EnrichmentSet{})
	assert.Nil(t, err)

	rows, err := buildDBPointsInflux(preparedPointGroups, &cfg, nil)
	assert.Nil(t, err)

	expected := []general.Point{
		{
			Measurement: "metric",
			Fields: map[string]interface{}{
				"value": "value_value",
				"warn":  "warn_value",
			},
			Tags: map[string]string{
				"added_tag": "added_tag_value",
				"host":      "host_value",
			},
			Timestamp: t1,
		},
		{
			Measurement: "metric",
			Fields: map[string]interface{}{
				"value": "value_value",
				"crit":  "crit_value",
			},
			Tags: map[string]string{
				"added_tag": "added_tag_value",
				"host":      "host_value",
				"service":   "service_value",
			},
			Timestamp: t2,
		},
		{
			Measurement: "state",
			Fields: map[string]interface{}{
				"warn": "warn_value",
			},
			Tags: map[string]string{
				"added_tag": "added_tag_value",
				"host":      "host_value",
			},
			Timestamp: t2,
		},
		{
			Measurement: "state",
			Fields: map[string]interface{}{
				"crit": "crit_value",
			},
			Tags: map[string]string{
				"added_tag":         "added_tag_value",
				"monitoringprofile": "monitoringprofile_value",
			},
			Timestamp: t2,
		},
	}
	assert.Equal(t, expected, rows)
}
