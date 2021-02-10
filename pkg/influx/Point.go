package influx

type Point struct {
	Measurement string
	Fields      map[string]interface{}
	Tags        map[string]string
	Timestamp   string
}

func New(measurement string, fields map[string]interface{}, tags map[string]string, timestamp string) Point {
	e := Point{measurement, fields, tags, timestamp}
	return e
}
