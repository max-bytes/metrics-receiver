package general

import (
	"fmt"

	"github.com/max-bytes/metrics-receiver/pkg/config"
	"github.com/max-bytes/metrics-receiver/pkg/enrichments"
)

func PreparePointGroups(i []PointGroup, cfg config.OutputConfig, enrichmentSets []config.EnrichmentSet) ([]PointGroup, error) {
	ret := make([]PointGroup, 0)
	for _, input := range i {
		var points = input.Points
		var measurement = input.Measurement

		// find measurement config
		if _, ok := cfg.GetMeasurementConfig(measurement); !ok {
			return nil, fmt.Errorf("Unknown measurement \"%s\" encountered", measurement)
		}
		measurementConfig, _ := cfg.GetMeasurementConfig(measurement)

		// find enrichment set
		enrichmentName := measurementConfig.GetEnrichment()
		var enrichmentSet *config.EnrichmentSet
		if enrichmentName != "" {
			var enrichmentSetErr error
			enrichmentSet, enrichmentSetErr = enrichments.FindEnrichmentSetByName(enrichmentName, enrichmentSets)
			if enrichmentSetErr != nil {
				return nil, fmt.Errorf("Unknown enrichment \"%s\" encountered", measurementConfig.GetEnrichment())
			}
		}

		// if this measurement should be ignored, continue with next point group
		if measurementConfig.GetIgnore() {
			continue
		}

		// filter points (if active)
		if !measurementConfig.GetIgnoreFiltering() {
			points = filterPoints(points, cfg)
		}

		// no points means an empty point group, which we can ignore
		if len(points) == 0 {
			continue
		}

		// enrich points
		var enrichedPoints []Point
		for _, point := range points {

			var tags = point.Tags

			if enrichmentSet != nil {
				var err error
				tags, err = enrichments.EnrichTags(tags, enrichmentSet)
				if err != nil {
					return nil, err
				}
			}

			for k, v := range measurementConfig.GetAddedTags() {
				tags[k] = v
			}

			enrichedPoints = append(enrichedPoints, Point{
				Measurement: point.Measurement,
				Fields:      point.Fields,
				Tags:        tags,
				Timestamp:   point.Timestamp})
		}

		ret = append(ret, PointGroup{
			Measurement: measurement,
			Points:      enrichedPoints,
		})
	}
	return ret, nil
}

func filterPoints(points []Point, c config.OutputConfig) []Point {

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
