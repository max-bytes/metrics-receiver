package influx

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
)

func TestBasicFunctionality(t *testing.T) {
	lines := []string{
		"# comment",
		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
		"weather,location=us-midwest temperature=82",                     // no timestamp
		"# comment",
		"weather2,location=us-midwest,source=test-source temperature=82,foo=12.3,bar=-1202.23 1465839830100400201"}

	currentTime := time.Now()
	actual, _ := Parse(strings.Join(lines, "\n"), currentTime)

	expected := []general.Point{
		{Measurement: "weather", Fields: map[string]interface{}{"temperature": 82}, Tags: map[string]string{"location": "us-midwest"}, Timestamp: time.Unix(0, int64(1465839830100400200))},
		{Measurement: "weather", Fields: map[string]interface{}{"temperature": 82}, Tags: map[string]string{"location": "us-midwest"}, Timestamp: currentTime}, // make this nil
		{Measurement: "weather2", Fields: map[string]interface{}{"temperature": 82, "foo": 12.3, "bar": -1202.23}, Tags: map[string]string{"location": "us-midwest", "source": "test-source"}, Timestamp: time.Unix(0, int64(1465839830100400201))},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func TestEscaping1(t *testing.T) {
	lines := []string{
		"weat\\,he\\ r,loc\\\"ation\\,\\ =us\\ mid\\\"west temperature=82,temperature_string=\"hot, really \\\"hot\\\"!\" 1465839830100400200", // all kinds of crazy characters
		"\"weather\",\"location\"=\"us-midwest\" \"temperature\"=82 1465839830100400200",                                                       // needlessly quoting of measurement, tag-keys, tag-values and field keys
	}

	currentTime := time.Now()
	actual, _ := Parse(strings.Join(lines, "\n"), currentTime)

	expected := []general.Point{
		{Measurement: "weat,he r", Fields: map[string]interface{}{"temperature": 82, "temperature_string": `hot, really "hot"!`}, Tags: map[string]string{`loc"ation, `: `us mid"west`}, Timestamp: time.Unix(0, int64(1465839830100400200))},
		{Measurement: `"weather"`, Fields: map[string]interface{}{`"temperature"`: 82}, Tags: map[string]string{`"location"`: `"us-midwest"`}, Timestamp: time.Unix(0, int64(1465839830100400200))},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func TestEscaping2(t *testing.T) {
	lines := []string{
		"weat\\=her,location=us-midwest temperature_string=\"temp: hot\" 1465839830100400200",           // escaped "=" in measurement
		"weat\\=her,loc\\=ation=us-mi\\=dwest temp\\=erature_string=\"temp\\=hot\" 1465839830100400201", // escaped "=" everywhere
	}

	currentTime := time.Now()
	actual, _ := Parse(strings.Join(lines, "\n"), currentTime)

	expected := []general.Point{
		{Measurement: "weat=her", Fields: map[string]interface{}{`temperature_string`: `temp: hot`}, Tags: map[string]string{`location`: `us-midwest`}, Timestamp: time.Unix(0, int64(1465839830100400200))},
		{Measurement: "weat=her", Fields: map[string]interface{}{`temp=erature_string`: `temp=hot`}, Tags: map[string]string{`loc=ation`: `us-mi=dwest`}, Timestamp: time.Unix(0, int64(1465839830100400201))},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func TestIncorrectString(t *testing.T) {
	lines := []string{
		"assafasfasfasfafa",
	}

	currentTime := time.Now()
	_, err := Parse(strings.Join(lines, "\n"), currentTime)

	// error should not be nil here
	if err == nil {
		t.Log("Error should not be nil")
		t.Fail()
	}
}

func TestIntValue(t *testing.T) {

	lines := []string{
		"value,label=state,customer=stark,host=xyz.com,service=test-service value=0i 1613985840702344400", // escaped "=" everywhere
	}

	currentTime := time.Now()
	actual, _ := Parse(strings.Join(lines, "\n"), currentTime)

	expected := []general.Point{
		{Measurement: "value", Fields: map[string]interface{}{`value`: int64(0)}, Tags: map[string]string{`label`: `state`, "customer": `stark`, `host`: `xyz.com`, `service`: `test-service`}, Timestamp: time.Unix(0, int64(1613985840702344400))},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func BenchmarkBasicFunctionality(b *testing.B) {
	potentialLines := []string{
		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
		"weather,location=us-midwest temperature=82",                     // no timestamp
		"weather2,location=us-midwest,source=test-source temperature=82,foo=12.3,bar=-1202.23 1465839830100400201"}

	numLines := 1000
	lines := make([]string, numLines)
	for i := 0; i < numLines; i++ {
		index := i % len(potentialLines)
		lines[i] = potentialLines[index]
	}
	str := strings.Join(lines, "\n")

	currentTime := time.Now()

	b.ResetTimer()

	_, _ = Parse(str, currentTime)
}
