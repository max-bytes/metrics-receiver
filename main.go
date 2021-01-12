package main

import (
	"fmt"
	"strings"

	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
)

func main() {
	fmt.Println("Hello, World!")

	t := []string{
		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
		"weather,location=us-midwest temperature=82",                     // no timestamp
		"weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"}

	// t := []string{
	// 	"weat\\,he\\ r,loc\\\"ation\\,\\ =us\\ mid\\\"west temperature=82,temperature_string=\"hot, really \\\"hot\\\"!\" 1465839830100400200", // all kinds of crazy characters
	// 	// "\"weather\",\"location\"=\"us-midwest\" \"temperature\"=82 1465839830100400200",                                                       // needlessly quoting of measurement, tag-keys, tag-values and field keys
	// }

	// t := []string{
	// 	// "weat\\=her,location=us-midwest temperature_string=\"temp: hot\" 1465839830100400200",           // escaped "=" in measurement
	// 	"weat\\=her,loc\\=ation=us-mi\\=dwest temp\\=erature_string=\"temp\\=hot\" 1465839830100400201", // escaped "=" everywhere
	// }

	// res := parse(strings.Join(t, "\n"))
	influx.Parse(strings.Join(t, "\n"))

	// fmt.Printf(fmt.Sprintf("%#v", res))
}
