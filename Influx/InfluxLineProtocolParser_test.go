package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestBasicFunctionality(t *testing.T) {
	lines := []string{
		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
		"weather,location=us-midwest temperature=82",                     // no timestamp
		"weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"}

	actual := parse(strings.Join(lines, "\n"))

	// new Point('weather', ['temperature' => 82], ['location' => 'us-midwest'], '1465839830100400200'),
	// new Point('weather', ['temperature' => 82], ['location' => 'us-midwest'], null),
	// new Point('weather2', ['temperature' => 82, 'foo' => 12.3, 'bar' => -1202.23], ['location' => 'us-midwest', 'source' => 'test-source'], '1465839830100400201'),

	expected := []Point{
		Point{"weather", map[string]interface{}{"temperature": 82}, map[string]string{"location": "us-midwest"}, "1465839830100400200"},
		Point{"weather", map[string]interface{}{"temperature": 82}, map[string]string{"location": "us-midwest"}, ""}, // make this nil
		Point{"weather2", map[string]interface{}{"temperature": 82, "foo": 12.3, "bar": -1202.23}, map[string]string{"location": "us-midwest", "source": "test-source"}, "1465839830100400201"},
	}

	fmt.Printf(fmt.Sprintf("%#v", actual))
	fmt.Printf(fmt.Sprintf("%#v", expected))

	// check if actual and expected are equal
}
