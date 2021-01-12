package influx

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicFunctionality(t *testing.T) {
	lines := []string{
		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
		"weather,location=us-midwest temperature=82",                     // no timestamp
		"weather2,location=us-midwest,source=test-source temperature=82,foo=12.3,bar=-1202.23 1465839830100400201"}

	actual, _ := Parse(strings.Join(lines, "\n"))

	expected := []Point{
		Point{"weather", map[string]interface{}{"temperature": 82}, map[string]string{"location": "us-midwest"}, "1465839830100400200"},
		Point{"weather", map[string]interface{}{"temperature": 82}, map[string]string{"location": "us-midwest"}, ""}, // make this nil
		Point{"weather2", map[string]interface{}{"temperature": 82, "foo": 12.3, "bar": -1202.23}, map[string]string{"location": "us-midwest", "source": "test-source"}, "1465839830100400201"},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func TestEscaping1(t *testing.T) {
	lines := []string{
		"weat\\,he\\ r,loc\\\"ation\\,\\ =us\\ mid\\\"west temperature=82,temperature_string=\"hot, really \\\"hot\\\"!\" 1465839830100400200", // all kinds of crazy characters
		"\"weather\",\"location\"=\"us-midwest\" \"temperature\"=82 1465839830100400200",                                                       // needlessly quoting of measurement, tag-keys, tag-values and field keys
	}

	actual, _ := Parse(strings.Join(lines, "\n"))

	expected := []Point{
		Point{"weat,he r", map[string]interface{}{"temperature": 82, "temperature_string": `hot, really "hot"!`}, map[string]string{`loc"ation, `: `us mid"west`}, "1465839830100400200"},
		Point{`"weather"`, map[string]interface{}{`"temperature"`: 82}, map[string]string{`"location"`: `"us-midwest"`}, "1465839830100400200"},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func TestEscaping2(t *testing.T) {
	lines := []string{
		"weat\\=her,location=us-midwest temperature_string=\"temp: hot\" 1465839830100400200",           // escaped "=" in measurement
		"weat\\=her,loc\\=ation=us-mi\\=dwest temp\\=erature_string=\"temp\\=hot\" 1465839830100400201", // escaped "=" everywhere
	}

	actual, _ := Parse(strings.Join(lines, "\n"))

	expected := []Point{
		Point{"weat=her", map[string]interface{}{`temperature_string`: `temp: hot`}, map[string]string{`location`: `us-midwest`}, "1465839830100400200"},
		Point{"weat=her", map[string]interface{}{`temp=erature_string`: `temp=hot`}, map[string]string{`loc=ation`: `us-mi=dwest`}, "1465839830100400201"},
	}

	assert.Equal(t, actual, expected, "The two objects should be the same.")
}

func TestIncorrectString(t *testing.T) {
	lines := []string{
		"assafasfasfasfafa",
	}

	_, err := Parse(strings.Join(lines, "\n"))

	// error should not be nil here
	if err == nil {
		t.Log("Error should not be nil")
		t.Fail()
	}
}
