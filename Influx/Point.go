package InfluxLineProtocolParser

type Point struct {
	// check types of these variables
	measurement string
	fields      map[string]interface{}
	tags        map[string]string
	// timestamp   *time.Time // change this should not be a string
	timestamp string // change this should not be a string
}

// type Point struct {
// 	Measurement string
// 	Tags        map[string]string
// 	Time        time.Time
// 	Fields      map[string]interface{}
// 	Precision   string
// 	Raw         string
// }

func New(measurement string, fields map[string]interface{}, tags map[string]string, timestamp string) Point {
	e := Point{measurement, fields, tags, timestamp}
	return e
}
