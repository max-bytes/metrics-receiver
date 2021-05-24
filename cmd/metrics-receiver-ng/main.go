package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/enrichments"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/influx"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/timescale"
)

var (
	version    = "0.0.0-src"
	configFile = flag.String("config", "config.json", "Config file location")
)

var cfg config.Configuration

var internalMetrics struct {
	incomingMetrics []general.Point

	incomingMessagesCount int64
	incomingLinesCount    int64
	incomingBytesCount    int64

	internalMetricsLock sync.Mutex
}

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.TraceLevel) // is overwritten by configuration below
}

func main() {

	logrus.Infof("metrics-receiver (Version: %s)", version)

	flag.Parse()

	logrus.Infof("Loading config from file: %s", *configFile)
	err := config.ReadConfigFromFile(*configFile, &cfg)
	if err != nil {
		logrus.Fatalf("Error opening config file: %s", err)
	}

	parsedLogLevel, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logrus.Fatalf("Error parsing loglevel in config file: %s", err)
	}
	logrus.SetLevel(parsedLogLevel)

	if cfg.Enrichment.CollectInterval > 0 {
		logrus.Infof("Started fetching enrichments...")
		err := enrichments.FetchEnrichments(cfg.Enrichment)
		if err != nil {
			logrus.Fatalf("Error trying to fetch data from omnikeeper: %s", err)
		}

		go func() {
			for range time.Tick(time.Duration(cfg.Enrichment.CollectInterval * int(time.Second))) {
				err := enrichments.FetchEnrichments(cfg.Enrichment)
				if err != nil {
					logrus.Errorf("Error trying to update enrichment chache: %s", err)
				}
			}
		}()
	} else {
		logrus.Infof("Not enriching metrics due to configuration")
	}

	if cfg.InternalMetricsCollectInterval > 0 && cfg.InternalMetricsFlushInterval > 0 {
		go func() {
			logrus.Infof("Started collecting internal metrics...")
			for now := range time.Tick(time.Duration(cfg.InternalMetricsCollectInterval * int(time.Second))) {
				internalMetrics.internalMetricsLock.Lock()

				metric := general.Point{
					Measurement: cfg.InternalMetricsMeasurement,
					Tags:        make(map[string]string),
					Fields: map[string]interface{}{
						"received_messages": internalMetrics.incomingMessagesCount,
						"received_lines":    internalMetrics.incomingLinesCount,
						"received_bytes":    internalMetrics.incomingBytesCount,
					},
					Timestamp: now,
				}
				internalMetrics.incomingMetrics = append(internalMetrics.incomingMetrics, metric)

				internalMetrics.incomingMessagesCount = 0
				internalMetrics.incomingLinesCount = 0
				internalMetrics.incomingBytesCount = 0

				internalMetrics.internalMetricsLock.Unlock()
				logrus.Debugf("Collected internal metrics")
			}
		}()

		go func() {
			logrus.Infof("Started sending internal metrics...")
			for range time.Tick(time.Duration(cfg.InternalMetricsFlushInterval * int(time.Second))) {
				logrus.Debugf("Sending internal metrics")
				internalMetrics.internalMetricsLock.Lock()

				// NOTE: we can't really treat critical errors different here, so we just log the error in both cases
				criticalError, nonCriticalErrors := writeOutputs(internalMetrics.incomingMetrics)
				if criticalError != nil {
					logrus.Errorf("Error writing internal metrics: %v", criticalError)
				}
				for _, nonCriticalError := range nonCriticalErrors {
					logrus.Errorf("Error writing internal metrics: %v", nonCriticalError)
				}

				internalMetrics.incomingMetrics = make([]general.Point, 0)
				internalMetrics.internalMetricsLock.Unlock()
			}
		}()
	} else {
		logrus.Infof("Not collecting or sending any internal metrics due to configuration")
	}

	http.HandleFunc("/api/influx/v1/write", influxWriteHandler)
	http.HandleFunc("/api/influx/v1/query", influxQueryHandler)
	http.HandleFunc("/api/health/check", healthCheckHandler)

	logrus.Infof("Starting server at port %d\n", cfg.Port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil); err != nil {
		logrus.Fatalf("Error opening config file: %s", err)
	}
}

// POST /influx/v1/write
func influxWriteHandler(w http.ResponseWriter, r *http.Request) {

	logrus.Infof("Receiving influx write request...")

	if r.Method != "POST" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	var reader io.ReadCloser
	var err error

	switch r.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			logrus.Errorf(err.Error())
			http.Error(w, "An error ocurred while trying to read the request body!", http.StatusBadRequest)
			return
		}
		defer reader.Close()
	default:
		reader = r.Body
	}

	buf, err := ioutil.ReadAll(reader)

	if err != nil {
		logrus.Errorf(err.Error())
		http.Error(w, "An error ocurred while trying to read the request body!", http.StatusBadRequest)
		return
	}

	requestStr := string(buf)

	points, parseErr := influx.Parse(requestStr, time.Now())
	if parseErr != nil {
		logrus.Errorf("An error occurred while parsing the influx line protocol request: " + parseErr.Error())
		http.Error(w, "An error occurred while parsing the influx line protocol request", http.StatusBadRequest)
		return
	}

	internalMetrics.internalMetricsLock.Lock()
	internalMetrics.incomingMessagesCount += 1
	internalMetrics.incomingBytesCount += int64(len(buf))
	internalMetrics.incomingLinesCount += int64(len(points))
	internalMetrics.internalMetricsLock.Unlock()

	criticalError, nonCriticalErrors := writeOutputs(points)
	if criticalError != nil {
		logrus.Errorf(criticalError.Error())
		http.Error(w, criticalError.Error(), http.StatusBadRequest)
		return
	} else {
		for _, nonCriticalError := range nonCriticalErrors {
			logrus.Errorf(nonCriticalError.Error())
		}

		logrus.Printf("Successfully processed influx write request; lines: %d \n", len(points))
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeOutputs(points []general.Point) (error, []error) {
	var pointGroups = measurementSplitter(points)
	var nonCriticalErrors []error

	// timescaledb outputs
	for _, outputConfig := range cfg.OutputsTimescale {
		enrichmentSet, enrichmentSetErr := findEnrichmentSetByName(outputConfig.EnrichmentType)
		if enrichmentSetErr != nil {
			return enrichmentSetErr, nonCriticalErrors
		}

		err := timescale.Write(pointGroups, &outputConfig, enrichmentSet)
		if err != nil {
			if outputConfig.WriteStrategy == "commit" {
				return fmt.Errorf("An error occurred writing timescaleDB output: %w", err), nonCriticalErrors
			} else {
				nonCriticalErrors = append(nonCriticalErrors, err)
			}
		}
	}

	// influxdb outputs
	for _, outputConfig := range cfg.OutputsInflux {
		enrichmentSet, enrichmentSetErr := findEnrichmentSetByName(outputConfig.EnrichmentType)
		if enrichmentSetErr != nil {
			return enrichmentSetErr, nonCriticalErrors
		}

		err := influx.Write(pointGroups, &outputConfig, enrichmentSet)
		if err != nil {
			if outputConfig.WriteStrategy == "commit" {
				return fmt.Errorf("An error occurred writing influxDB output: %w", err), nonCriticalErrors
			} else {
				nonCriticalErrors = append(nonCriticalErrors, err)
			}
		}
	}

	return nil, nonCriticalErrors
}

// GET /influx/v1/query
func influxQueryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	http.Error(w, "Not supported", http.StatusUnauthorized)
}

// GET /api/health/check
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func findEnrichmentSetByName(name string) (*config.EnrichmentSet, error) {
	if name == "" {
		return nil, nil
	}

	for _, v := range cfg.Enrichment.Sets {
		if name == v.Name {
			return &v, nil
		}
	}

	err := fmt.Sprintf("The configured enrichmentset {%s} could not be found!", name)
	return nil, errors.New(err)
}

func measurementSplitter(input []general.Point) []general.PointGroup {

	var groupedPoints map[string][]general.Point = make(map[string][]general.Point)

	for _, point := range input {
		var measurement = point.Measurement
		groupedPoints[measurement] = append(groupedPoints[measurement], point)
	}

	var r []general.PointGroup

	for measurement, points := range groupedPoints {
		var p = general.PointGroup{Measurement: measurement, Points: points}
		r = append(r, p)
	}

	return r
}
