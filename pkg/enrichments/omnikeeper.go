package enrichments

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"

	"golang.org/x/oauth2"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	okclient "www.mhx.at/gitlab/landscape/omnikeeper-client-go.git"
)

var apiVersion = "1"

func FetchEnrichments(cfg config.EnrichmentSets) error {

	for _, enrichmentSet := range cfg.Sets {
		result, err := getCisByTrait(enrichmentSet, cfg)
		if err != nil {
			enrichmentsCache.RetryCount += 1

			if enrichmentsCache.RetryCount > cfg.RetryCount {
				enrichmentsCache.IsValid = false
			}

			return err
		} else {
			enrichmentsCache.RetryCount = 0
			enrichmentsCache.IsValid = true
		}

		updateEnrichmentCache(result, enrichmentSet.Name)
	}

	return nil
}

func updateEnrichmentCache(result map[string]okclient.EffectiveTraitDTO, setName string) {

	var enrichmentItems []map[string]string
	for _, value := range result {
		item := make(map[string]string)
		for _, v := range value.TraitAttributes {
			if !v.Value.IsArray {
				item[v.Name] = v.Value.Values[0]
			} else {
				item[v.Name] = strings.Join(v.Value.Values[:], ",")
			}
		}
		enrichmentItems = append(enrichmentItems, item)
	}

	enrichmentsCache.EnrichmentItems[setName] = enrichmentItems
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
	token, tokenErr := oauth2cfg.PasswordCredentialsToken(ctx, cfgFull.Username, cfgFull.Password)

	if tokenErr != nil {
		return nil, tokenErr
	}

	configuration := okclient.NewConfiguration()
	configuration.Servers[0].URL = cfgFull.ServerURL
	api_client := okclient.NewAPIClient(configuration)

	tokenSource := oauth2cfg.TokenSource(ctx, token)
	auth := context.WithValue(ctx, okclient.ContextOAuth2, tokenSource)
	resp, _, err := api_client.TraitApi.GetEffectiveTraitsForTraitName(auth, apiVersion).LayerIDs(cfg.LayerIds).TraitName(cfg.TraitName).Execute()

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func EnrichMetrics(tags map[string]string, enrichmentSet config.EnrichmentSet) (map[string]string, error) {
	if !enrichmentsCache.IsValid {
		return nil, errors.New("Failed to enirich metrics dute to invalid enrichments cache!")
	}

	if _, ok := tags[enrichmentSet.LookupTag]; ok {
		// skip enrichments in case of internal metrics
		if !reflect.DeepEqual(enrichmentSet, config.EnrichmentSet{}) {
			if lookupTagValue, ok := tags[enrichmentSet.LookupTag]; ok {
				var traitAttributes = enrichmentsCache.EnrichmentItems[enrichmentSet.Name]
				for _, attributes := range traitAttributes {
					if value, ok := attributes[enrichmentSet.LookupAttribute]; value != lookupTagValue || !ok {
						continue
					}

					for k, v := range attributes {
						if k != enrichmentSet.LookupAttribute {
							tags[k] = v
						}
					}

					break
				}
			}
		}
	}

	return tags, nil
}

var enrichmentsCache *Cache = &Cache{
	EnrichmentItems: map[string][]map[string]string{},
	CacheLock:       sync.Mutex{},
}

type Cache struct {
	EnrichmentItems map[string][]map[string]string
	RetryCount      int
	IsValid         bool
	CacheLock       sync.Mutex
}
