package enrichments

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"github.com/sirupsen/logrus"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
)

var retryCount = 0

func EnrichMetrics(cfg config.EnrichmentSets) {
	// for range time.Tick(time.Duration(cfg.CollectInterval * int(time.Second))) {

	for _, enrichmentSet := range cfg.Sets {
		result, err := getCisByTrait(enrichmentSet)
		if err != nil {
			if retryCount == cfg.RetryCount {
				logrus.Fatalf("Could not connect for the %v time to omnikeeper", retryCount)
			}
			retryCount += 1
			logrus.Infof(err.Error())
			continue
		}

		var enerichmentItems map[string]map[string]string = make(map[string]map[string]string)

		for key, value := range result {
			// attributes
			var i map[string]string = make(map[string]string)
			for k, v := range value.TraitAttributes {
				if !v.Value.IsArray {
					i[k] = v.Value.Values[0]
				}
			}

			enerichmentItems[key] = i
		}

		SetEnrichmentsCacheValues(enrichmentSet.Name, enerichmentItems)
	}
	// }
}

func getCisByTrait(cfg config.EnrichmentSet) (map[string]Trait, error) {
	// /api/v{version}/Trait/getEffectiveTraitsForTraitName
	params := url.Values{
		"traitName": {cfg.TraitName},
		"layerIDs":  intListToStringList(cfg.LayerIds),
	}

	url := cfg.BaseUrl + `/api/v1.0/Trait/getEffectiveTraitsForTraitName?` + params.Encode()
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

	var result EfectiveTraitsResponse
	err = json.Unmarshal(byteValue, &result.Traits)
	if err != nil {
		return nil, fmt.Errorf("can't parse config file: %w", err)
	}

	return result.Traits, nil
}

func intListToStringList(a []int) []string {
	var b []string
	for _, val := range a {
		b = append(b, fmt.Sprint(val))
	}

	return b
}

var enrichmentsCache *Cache = &Cache{
	EnrichmentItems: map[string]map[string]map[string]string{},
	CacheLock:       sync.Mutex{},
}

// "minimal": {
// 	"123213": {
// 		"hostname": "value"
// 	}
// }

type Cache struct {
	EnrichmentItems map[string]map[string]map[string]string
	CacheLock       sync.Mutex
}

func GetEnrichmentsCache() *Cache {
	return enrichmentsCache
}

func SetEnrichmentsCacheValues(key string, values map[string]map[string]string) {
	enrichmentsCache.EnrichmentItems[key] = values
}
