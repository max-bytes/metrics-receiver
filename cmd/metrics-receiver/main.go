package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/max-bytes/metrics-receiver/pkg/config"
	"github.com/max-bytes/metrics-receiver/pkg/enrichments"
	"github.com/max-bytes/metrics-receiver/pkg/general"
	"github.com/max-bytes/metrics-receiver/pkg/influx"
	"github.com/max-bytes/metrics-receiver/pkg/timescale"
	"github.com/sirupsen/logrus"
)

var (
	version    = "0.0.0-src"
	configFile = flag.String("config", "C:\\Users\\MuhametKaçuri\\repos\\metrics-receiver-ng\\config-example.json", "Config file location")
	log        logrus.Logger
)

// default configuration
var cfg = config.Configuration{
	LogLevel:                       "info",
	InternalMetricsMeasurement:     "metrics_receiver",
	Port:                           80,
	InternalMetricsCollectInterval: 60,
	InternalMetricsFlushCycle:      1,
	OutputsTimescale:               []config.OutputTimescale{},
	OutputsInflux:                  []config.OutputInflux{},
}

var internalMetrics struct {
	incomingMetrics []general.Point

	incomingMessagesCount int64
	incomingLinesCount    int64
	incomingBytesCount    int64

	internalMetricsLock sync.Mutex
}

func init() {
	log = *logrus.StandardLogger()
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetLevel(logrus.TraceLevel) // is overwritten by configuration below
}

func main() {

	log.Infof("metrics-receiver (Version: %s)", version)

	flag.Parse()

	log.Infof("Loading config from file: %s", *configFile)
	err := config.ReadConfigFromFile(*configFile, &cfg)
	if err != nil {
		log.Fatalf("Error opening config file: %s", err)
	}

	parsedLogLevel, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		log.Fatalf("Error parsing loglevel in config file: %s", err)
	}
	log.SetLevel(parsedLogLevel)

	// init timescale connection pools
	connPoolsErr := timescale.InitConnPools(cfg.OutputsTimescale)

	if connPoolsErr != nil {
		log.Fatalf("Failed to init connection pools for timecaledb: %s", connPoolsErr)
	}

	if cfg.Enrichment.CollectInterval > 0 {
		log.Infof("Started fetching enrichments...")
		err := enrichments.FetchEnrichments(cfg.Enrichment)
		if err != nil {
			log.Fatalf("Error trying to fetch data from omnikeeper: %s", err)
		} else {
			log.Debug("Fetched enrichments")
		}

		go func() {
			for range time.Tick(time.Duration(cfg.Enrichment.CollectInterval * int(time.Second))) {
				log.Debug("Fetching enrichments")
				err := enrichments.FetchEnrichments(cfg.Enrichment)
				if err != nil {
					log.Errorf("Error trying to update enrichment cache: %s", err)
				} else {
					log.Debug("Fetched enrichments")
				}
			}
		}()
	} else {
		log.Infof("Not enriching metrics due to configuration")
	}

	if cfg.InternalMetricsCollectInterval > 0 && cfg.InternalMetricsFlushCycle > 0 {
		go func() {
			log.Infof("Started collecting internal metrics...")
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
				log.Debugf("Collected internal metrics")
			}
		}()

		go func() {
			log.Infof("Started sending internal metrics...")
			for range time.Tick(time.Duration(cfg.InternalMetricsFlushCycle * int(time.Second))) {
				internalMetrics.internalMetricsLock.Lock()

				// NOTE: we can't really treat critical errors different here, so we just log the error in both cases
				criticalError, nonCriticalErrors := writeOutputs(internalMetrics.incomingMetrics)
				for _, nonCriticalError := range nonCriticalErrors {
					log.Warnf("Non-critical error writing internal metrics: %v", nonCriticalError)
				}
				if criticalError != nil {
					log.Errorf("Critical error writing internal metrics: %v", criticalError)
				} else {
					log.Debugf("Sent internal metrics")
				}

				internalMetrics.incomingMetrics = make([]general.Point, 0)
				internalMetrics.internalMetricsLock.Unlock()
			}
		}()
	} else {
		log.Infof("Not collecting or sending any internal metrics due to configuration")
	}

	http.HandleFunc("/api/influx/v1/write", influxWriteHandler)
	http.HandleFunc("/api/influx/v1/query", influxQueryHandler)
	http.HandleFunc("/api/health/check", healthCheckHandler)
	http.HandleFunc("/api/enrichment/cacheinfo", enrichmentCacheInfoHandler)
	http.HandleFunc("/api/enrichment/cacheinfo/items", enrichmentCacheItemsInfoHandler)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	log.Infof("Starting server at port %d\n", cfg.Port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.Port), nil); err != nil {
		log.Fatalf("Error starting metrics-receiver: %s", err)
	}

	<-done
	timescale.CloseConnectionPools()
	fmt.Println("closed connection pools")
}

// POST /influx/v1/write
func influxWriteHandler(w http.ResponseWriter, r *http.Request) {

	log.Infof("Receiving influx write request...")

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
			log.Errorf(err.Error())
			http.Error(w, "An error ocurred while trying to read the request body!", http.StatusBadRequest)
			return
		}
		defer reader.Close()
	default:
		reader = r.Body
	}

	buf, err := ioutil.ReadAll(reader)

	if err != nil {
		log.Errorf(err.Error())
		http.Error(w, "An error ocurred while trying to read the request body!", http.StatusBadRequest)
		return
	}

	requestStr := string(buf)

	points, parseErr := influx.Parse(requestStr, time.Now())
	if parseErr != nil {
		log.Errorf("An error occurred while parsing the influx line protocol request: " + parseErr.Error())
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
		log.Errorf(criticalError.Error())
		http.Error(w, criticalError.Error(), http.StatusBadRequest)
		return
	} else {
		for _, nonCriticalError := range nonCriticalErrors {
			log.Warnf(nonCriticalError.Error())
		}

		log.Printf("Successfully processed influx write request; lines: %d \n", len(points))
		w.WriteHeader(http.StatusNoContent)
	}
}

func writeOutputs(points []general.Point) (error, []error) {
	var pointGroups = general.SplitPointsByMeasurement(points)
	var nonCriticalErrors []error

	// timescaledb outputs
	for _, outputConfig := range cfg.OutputsTimescale {
		preparedPoints, err := general.PreparePointGroups(pointGroups, &outputConfig, cfg.Enrichment.Sets, &log)
		if err != nil {
			if outputConfig.WriteStrategy == "commit" {
				return fmt.Errorf("An error occurred preparing timescaleDB output: %w", err), nonCriticalErrors
			} else {
				nonCriticalErrors = append(nonCriticalErrors, err)
				continue
			}
		}

		// add connection pool here
		err = timescale.Write(preparedPoints, &outputConfig, cfg.Enrichment.Sets)
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
		preparedPoints, err := general.PreparePointGroups(pointGroups, &outputConfig, cfg.Enrichment.Sets, &log)
		if err != nil {
			if outputConfig.WriteStrategy == "commit" {
				return fmt.Errorf("An error occurred preparing influxDB output: %w", err), nonCriticalErrors
			} else {
				nonCriticalErrors = append(nonCriticalErrors, err)
				continue
			}
		}

		err = influx.Write(preparedPoints, &outputConfig, cfg.Enrichment.Sets)
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
	http.Error(w, "Not supported", http.StatusForbidden)
}

// GET /api/health/check
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	if !enrichments.GetEnrichmentCache().IsValid {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "Invalid enrichment cache")
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

// GET /api/enrichment/cacheinfo
func enrichmentCacheInfoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	enrichmentCache := enrichments.GetEnrichmentCache()
	items := enrichmentCache.EnrichmentItems
	itemKeys := make([]string, len(items))
	i := 0
	for k := range items {
		itemKeys[i] = k
		i++
	}

	output := map[string]interface{}{
		"retryCount": enrichmentCache.RetryCount,
		"lastUpdate": enrichmentCache.LastUpdate,
		"cacheItems": itemKeys,
		"isValid":    enrichmentCache.IsValid,
	}

	jsonEncoder := json.NewEncoder(w)
	jsonEncoder.Encode(output)
}

// GET /api/enrichment/cacheinfo/items
func enrichmentCacheItemsInfoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method is not supported.", http.StatusForbidden)
		return
	}

	enrichmentCache := enrichments.GetEnrichmentCache()
	items := enrichmentCache.EnrichmentItems

	jsonEncoder := json.NewEncoder(w)
	jsonEncoder.Encode(items)
}
