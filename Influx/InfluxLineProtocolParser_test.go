package Influx

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

	points := parse(strings.Join(lines, "\n"))

	// should := Point[
	// 	Point{measurement, fieldSet, tagSet, timestamp}
	// ]

	should := []Point{
		Point{"weather", ["temperature" => 82], ["location" => "us-midwest"], "1465839830100400200"},
		Point{"weather", ["temperature" => 82], ["location" => "us-midwest"], null},
		Point{"weather2", ["temperature" => 82, "foo" => 12.3, "bar" => -1202.23], ["location" => "us-midwest", "source" => "test-source"], "1465839830100400201")}

	}
	fmt.Printf(fmt.Sprintf("%#v", points))
}
