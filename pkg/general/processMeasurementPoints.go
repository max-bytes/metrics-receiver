package general

import (
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/enrichments"
)

func ProcessMeasurementPoints(points []Point, measurementConfig config.MeasurementConfig, outputConfig config.Tagfilter, enrichmentSet *config.EnrichmentSet) ([]Point, error) {

	if measurementConfig.GetIgnore() {
		return []Point{}, nil
	}

	if !measurementConfig.GetIgnoreFiltering() {
		points = filterPoints(points, outputConfig)
	}

	var addedTags map[string]string = nil
	if measurementConfig.GetAddedTags() != nil {
		addedTags = measurementConfig.GetAddedTags()
	}

	var outputPoints []Point
	for _, point := range points {

		var tags = point.Tags

		tags, err := enrichments.EnrichMetrics(tags, enrichmentSet)
		if err != nil {
			return nil, err
		}

		for k, v := range addedTags {
			tags[k] = v
		}

		outputPoints = append(outputPoints, Point{
			Measurement: point.Measurement,
			Fields:      point.Fields,
			Tags:        tags,
			Timestamp:   point.Timestamp})
	}
	return outputPoints, nil
}

func filterPoints(points []Point, c config.Tagfilter) []Point {

	var tagfilterInclude map[string][]string = c.GetTagfilterInclude()
	var filteredPoints []Point
	if len(tagfilterInclude) == 0 {
		// no filtering
		filteredPoints = points
	} else {
		for _, point := range points {
		outInclude:
			for tagKey, tagValue := range point.Tags {
				if _, ok := tagfilterInclude[tagKey]; ok {
					for _, v := range tagfilterInclude[tagKey] {
						if v == "*" || tagValue == v {
							filteredPoints = append(filteredPoints, point)
							break outInclude
						}
					}
				}
			}
		}
	}

	var tagfilterBlock map[string][]string = c.GetTagfilterBlock()
	var keysToDelete []int
	for pointKey, point := range filteredPoints {
	outBlock:
		for tagKey, tagValue := range point.Tags {
			if _, ok := tagfilterBlock[tagKey]; ok {
				for _, v := range tagfilterBlock[tagKey] {
					if v == "*" || tagValue == v {
						// index of the value to remove from points array
						keysToDelete = append(keysToDelete, pointKey)
						break outBlock
					}
				}
			}
		}
	}

	var result []Point
	for pointKey := range filteredPoints {
		if !contains(keysToDelete, pointKey) {
			result = append(result, filteredPoints[pointKey])
		}
	}

	return result
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
