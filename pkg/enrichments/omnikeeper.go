package enrichments

import (
	"context"
	"errors"
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
	enrichmentsCache.CacheLock.Lock()
	enrichmentsCache.EnrichmentItems[setName] = enrichmentItems
	enrichmentsCache.CacheLock.Unlock()
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

func EnrichMetrics(tags map[string]string, enrichmentSet *config.EnrichmentSet) (map[string]string, error) {
	if enrichmentSet == nil { // no/nil enrichment set specified, do no enrichment and return passed in tags
		return tags, nil
	}

	if !enrichmentsCache.IsValid {
		return nil, errors.New("Failed to enrich metrics due to invalid enrichments cache!")
	}

	if lookupTagValue, ok := tags[enrichmentSet.LookupTag]; ok {
		var tagsCopy map[string]string = make(map[string]string)

		for k, v := range tags {
			tagsCopy[k] = v
		}

		enrichmentsCache.CacheLock.RLock()
		var traitAttributes = enrichmentsCache.EnrichmentItems[enrichmentSet.Name]
		for _, attributes := range traitAttributes {
			// TODO: this is really slow, consider replacing it with a map+lookup
			if value, ok := attributes[enrichmentSet.LookupAttribute]; value != lookupTagValue || !ok {
				continue
			}

			for k, v := range attributes {
				if k != enrichmentSet.LookupAttribute {
					tagsCopy[k] = v
				}
			}

			break
		}
		enrichmentsCache.CacheLock.RUnlock()

		return tagsCopy, nil
	}

	// if there is nothing to enrich return the passed in tags
	return tags, nil
}

func ForceSetEnrichmentCache(enrichmentItems []map[string]string, cacheEntryName string) {
	enrichmentsCache.CacheLock.Lock()
	enrichmentsCache.EnrichmentItems[cacheEntryName] = enrichmentItems
	enrichmentsCache.IsValid = true
	enrichmentsCache.CacheLock.Unlock()
}

var enrichmentsCache *Cache = &Cache{
	EnrichmentItems: map[string][]map[string]string{},
	CacheLock:       sync.RWMutex{},
}

type Cache struct {
	EnrichmentItems map[string][]map[string]string
	RetryCount      int
	IsValid         bool
	CacheLock       sync.RWMutex
}
