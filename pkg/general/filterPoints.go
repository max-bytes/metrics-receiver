package general

import (
	"fmt"

	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
)

func FilterPoints(points []Point, c interface{}) []Point {

	var tagfilterInclude map[string][]string = make(map[string][]string)
	var tagfilterBlock map[string][]string = make(map[string][]string)
	switch v := c.(type) {
	case config.OutputInflux:
		tagfilterInclude = v.TagfilterInclude
		tagfilterBlock = v.TagfilterBlock
	case config.OutputTimescale:
		tagfilterInclude = v.TagfilterInclude
		tagfilterBlock = v.TagfilterBlock
	default:
		fmt.Printf("I don't know about type %T!\n", v)
	}

	var filteredPoints []Point
	if len(tagfilterInclude) == 0 {
		// no filtering
		filteredPoints = points
	} else {
		for _, point := range points {
		outInclude:
			for tagKey, tagValue := range point.Tags {
				if _, ok := tagfilterInclude[tagKey]; ok == true {
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

	var keysToDelete []int
	for pointKey, point := range filteredPoints {

	outBlock:
		for tagKey, tagValue := range point.Tags {
			if _, ok := tagfilterBlock[tagKey]; ok == true {
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
		if contains(keysToDelete, pointKey) == false {
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
