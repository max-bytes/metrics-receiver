package influx

import (
	"context"
	"errors"
	"fmt"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
)

func Write(groupedPoints []general.PointGroup, config *config.OutputInflux) error {
	var points, err = buildDBPointsInflux(groupedPoints, config)

	if err != nil {
		return fmt.Errorf("An error ocurred while building db rows: %w", err)
	}

	if len(points) > 0 {
		insertErr := insertRowsInflux(points, config)
		if insertErr != nil {
			return fmt.Errorf("An error ocurred while inserting db rows: %w", insertErr)
		}
	}
	return nil
}

func buildDBPointsInflux(i []general.PointGroup, config *config.OutputInflux) ([]general.Point, error) {
	var writePoints []general.Point
	for _, input := range i {
		var points = input.Points
		var measurement = input.Measurement

		if _, ok := config.Measurements[measurement]; ok == false {
			return nil, errors.New(fmt.Sprintf("Unknown measurement \"%s\" encountered", measurement))
		}

		var measurementConfig = config.Measurements[measurement]

		if measurementConfig.Ignore {
			continue
		}

		var addedTags map[string]string = nil

		if measurementConfig.AddedTags != nil {
			addedTags = measurementConfig.AddedTags
		}

		points = general.FilterPoints(points, config)

		for _, point := range points {

			var tags = point.Tags

			if addedTags != nil {
				for k, v := range addedTags {
					tags[k] = v
				}
			}

			writePoints = append(writePoints, general.Point{
				Measurement: measurement,
				Fields:      point.Fields,
				Tags:        tags,
				Timestamp:   point.Timestamp})

		}
	}

	return writePoints, nil
}

func insertRowsInflux(writePoints []general.Point, config *config.OutputInflux) error {

	// create new client with default option for server url authenticate by token
	client := influxdb2.NewClient(config.Connection, config.AuthToken)

	// user blocking write client for writes to desired bucket
	writeAPI := client.WriteAPIBlocking(config.Org, config.DbName)

	for _, p := range writePoints {
		p1 := influxdb2.NewPoint(p.Measurement,
			p.Tags,
			p.Fields,
			p.Timestamp)

		err := writeAPI.WritePoint(context.Background(), p1)
		if err != nil {
			return err
		}
	}

	// Ensures background processes finish
	client.Close()
	return nil
}
