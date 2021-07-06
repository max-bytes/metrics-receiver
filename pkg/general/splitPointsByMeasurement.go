package general

func SplitPointsByMeasurement(input []Point) []PointGroup {

	var groupedPoints map[string][]Point = make(map[string][]Point)

	for _, point := range input {
		var measurement = point.Measurement
		groupedPoints[measurement] = append(groupedPoints[measurement], point)
	}

	var r []PointGroup
	for measurement, points := range groupedPoints {
		var p = PointGroup{Measurement: measurement, Points: points}
		r = append(r, p)
	}

	return r
}
