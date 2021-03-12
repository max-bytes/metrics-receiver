package general

import "time"

type Point struct {
	Measurement string
	Fields      map[string]interface{}
	Tags        map[string]string
	Timestamp   time.Time
}
