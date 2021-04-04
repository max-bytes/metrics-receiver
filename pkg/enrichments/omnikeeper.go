package enrichments

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
)

var retryCount = 0

func EnrichMetrics(cfg config.EnrichmentSets) {
	for range time.Tick(time.Duration(cfg.CollectInterval * int(time.Second))) {
		result_minimal, minimal_err := getCiisByTrait(cfg.Minimal)
		if minimal_err != nil {
			if retryCount == cfg.RetryCount {
				logrus.Fatalf("Could not connect for the %v time to omnikeeper", retryCount)
			}
			retryCount += 1
			logrus.Infof(minimal_err.Error())
		}

		SetEnrichmentsCacheMinimal(result_minimal)

		result_full, full_err := getCiisByTrait(cfg.Minimal)
		if full_err != nil {
			if retryCount == cfg.RetryCount {
				logrus.Fatalf("Could not connect for the %v time to omnikeeper", retryCount)
			}
			retryCount += 1
			logrus.Infof(full_err.Error())
		}

		SetEnrichmentsCacheFull(result_full)
	}
}

func getCiisByTrait(cfg config.EnrichmentSet) (map[string]string, error) {
	url := cfg.BaseUrl + `/api/v1.0/CI/getAllCIIDs`
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
		// handle error
	}
	defer resp.Body.Close()
	byteValue, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("can't read config file: %w", err)
	}

	var result map[string]string
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return nil, fmt.Errorf("can't parse config file: %w", err)
	}

	fmt.Println(result)
	return result, nil
}

var enrichmentsCache *Cache = &Cache{
	Minimal: map[string]string{
		"ci_status": "placeholder",
		"ci_zone":   "placeholder",
	},
	Full: map[string]string{
		"ci_status":                "placeholder",
		"ci_zone":                  "placeholder",
		"ci_assigmentgroup":        "placeholder",
		"tagname_in_metric_stream": "placeholder",
	},
}

type Cache struct {
	Minimal   map[string]string
	Full      map[string]string
	CacheLock sync.Mutex
}

func GetEnrichmentsCache() *Cache {
	return enrichmentsCache
}

func SetEnrichmentsCacheMinimal(minimal map[string]string) {
	enrichmentsCache.CacheLock.Lock()
	enrichmentsCache.Minimal = minimal
	enrichmentsCache.CacheLock.Unlock()
}

func SetEnrichmentsCacheFull(full map[string]string) {
	enrichmentsCache.CacheLock.Lock()
	enrichmentsCache.Full = full
	enrichmentsCache.CacheLock.Unlock()
}
