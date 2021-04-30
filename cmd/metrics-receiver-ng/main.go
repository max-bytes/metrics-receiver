package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
		log.Fatalf("Error parsing loglevel in config file: %s", err)
	}
	logrus.SetLevel(parsedLogLevel)

	go func() {
		enrichments.EnrichMetrics(cfg.EnrichmentSets)

		for range time.Tick(time.Duration(cfg.EnrichmentSets.CollectInterval * int(time.Second))) {
			enrichments.EnrichMetrics(cfg.EnrichmentSets)
		}
	}()

	go func() {
		if cfg.InternalMetricsCollectInterval > 0 {
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
		} else {
			logrus.Infof("Not collecting internal metrics due to configuration")
		}
	}()

	go func() {
		if cfg.InternalMetricsFlushInterval > 0 {
			logrus.Infof("Started sending internal metrics...")
			for range time.Tick(time.Duration(cfg.InternalMetricsFlushInterval * int(time.Second))) {
				internalMetrics.internalMetricsLock.Lock()
				for _, outputConfig := range cfg.OutputsTimescale {
					var splittedRows = measurementSplitter(internalMetrics.incomingMetrics)

					err := timescale.Write(splittedRows, &outputConfig, config.EnrichmentSet{})

					if err != nil {
						logrus.Errorf("Error writing internal metrics to timescale: %v", err)
					}

				}

				for _, outputConfig := range cfg.OutputsInflux {
					var splittedRows = measurementSplitter(internalMetrics.incomingMetrics)

					err := influx.Write(splittedRows, &outputConfig, config.EnrichmentSet{})

					if err != nil {
						logrus.Errorf("Error writing internal metrics to influx: %v", err)
					}
				}

				internalMetrics.incomingMetrics = make([]general.Point, 0)
				internalMetrics.internalMetricsLock.Unlock()
				logrus.Debugf("Sent internal metrics")
			}
		} else {
			logrus.Infof("Not flushing internal metrics due to configuration")
		}
	}()

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

	res, parseErr := influx.Parse(requestStr, time.Now())
	if parseErr != nil {
		logrus.Errorf(err.Error())
		http.Error(w, "An error occurred while parsing the influx line protocol request", http.StatusBadRequest)
		return
	}

	internalMetrics.internalMetricsLock.Lock()
	internalMetrics.incomingMessagesCount += 1
	internalMetrics.incomingBytesCount += int64(len(buf))
	internalMetrics.incomingLinesCount += int64(len(res))
	internalMetrics.internalMetricsLock.Unlock()

	var splittedRows = measurementSplitter(res)

	// timescaledb outputs
	for _, outputConfig := range cfg.OutputsTimescale {
		enrichmentSet := findEnrichmentSetByName(outputConfig.EnrichmentType)

		err := timescale.Write(splittedRows, &outputConfig, enrichmentSet)

		if err != nil {
			logrus.Errorf(err.Error())
			if outputConfig.WriteStrategy == "commit" {
				http.Error(w, "An error occurred handling timescaleDB output: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	// influxdb outputs
	for _, outputConfig := range cfg.OutputsInflux {
		enrichmentSet := findEnrichmentSetByName(outputConfig.EnrichmentType)

		err := influx.Write(splittedRows, &outputConfig, enrichmentSet)

		if err != nil {
			logrus.Errorf(err.Error())
			if outputConfig.WriteStrategy == "commit" {
				http.Error(w, "An error occurred handling influxDB output: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
	}

	log.Printf("Successfully processed influx write request; lines: %d \n", len(res))

	w.WriteHeader(http.StatusNoContent)
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

func findEnrichmentSetByName(name string) config.EnrichmentSet {
	for _, v := range cfg.EnrichmentSets.Sets {
		if name == v.Name {
			return v
		}
	}
	return config.EnrichmentSet{} // nothing found, return an empty set

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
