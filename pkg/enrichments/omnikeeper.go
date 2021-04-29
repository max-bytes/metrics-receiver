package enrichments

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	okclient "www.mhx.at/gitlab/landscape/omnikeeper-client-go.git"
)

var retryCount = 0
var apiVersion = "1"

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func EnrichMetrics(cfg config.EnrichmentSets) {
	for _, enrichmentSet := range cfg.Sets {
		result, err := getCisByTrait(enrichmentSet, cfg)
		if err != nil {
			if retryCount == cfg.RetryCount {
				logrus.Fatalf("Could not connect for the %v time to omnikeeper", retryCount)
			}
			retryCount += 1
			logrus.Infof(err.Error())
			continue
		}

		var enerichmentItems []map[string]string
		for _, value := range result {
			item := make(map[string]string)
			for _, v := range value.TraitAttributes {
				if !v.Value.IsArray {
					item[v.Name] = v.Value.Values[0]
				} else {
					item[v.Name] = strings.Join(v.Value.Values[:], ",")
				}
			}
			enerichmentItems = append(enerichmentItems, item)
		}

		SetEnrichmentsCacheValues(enrichmentSet.Name, enerichmentItems)
	}
}

func getCisByTrait(cfg config.EnrichmentSet, cfgFull config.EnrichmentSets) (map[string]okclient.EffectiveTraitDTO, error) {

	oauth2cfg := &oauth2.Config{
		ClientID: cfgFull.ClientID,
		Endpoint: oauth2.Endpoint{
			AuthURL:  cfgFull.AuthURL,
			TokenURL: cfgFull.TokenURL,
		},
	}

	ctx := context.Background()
	token, err := oauth2cfg.PasswordCredentialsToken(ctx, cfgFull.Username, cfgFull.Password)
	exitOnError(err)

	configuration := okclient.NewConfiguration()
	configuration.Servers[0].URL = cfgFull.ServerURL
	api_client := okclient.NewAPIClient(configuration)

	tokenSource := oauth2cfg.TokenSource(ctx, token)
	auth := context.WithValue(ctx, okclient.ContextOAuth2, tokenSource)
	resp, r, err := api_client.TraitApi.GetEffectiveTraitsForTraitName(auth, apiVersion).LayerIDs(cfg.LayerIds).TraitName(cfg.TraitName).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TraitApi.GetEffectiveTraitsForTraitName``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
		return nil, err
	}

	return resp, nil
}

var enrichmentsCache *Cache = &Cache{
	EnrichmentItems: map[string][]map[string]string{},
	CacheLock:       sync.Mutex{},
}

type Cache struct {
	EnrichmentItems map[string][]map[string]string
	CacheLock       sync.Mutex
}

func GetEnrichmentsCache() *Cache {
	return enrichmentsCache
}

func SetEnrichmentsCacheValues(key string, values []map[string]string) {
	enrichmentsCache.EnrichmentItems[key] = values
}
