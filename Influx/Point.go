package main

type Point struct {
	// check types of these variables
	measurement string
	fields      []string
	tags        []string
	timestamp   string
}

func New(measurement string, fields []string, tags []string, timestamp string) Point {
	e := Point{measurement, fields, tags, timestamp}
	return e
}
