package enrichments

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/max-bytes/metrics-receiver/pkg/config"
	okclient "github.com/max-bytes/omnikeeper-client-go"
	"golang.org/x/oauth2"
)

var apiVersion = "1"

func FetchEnrichments(cfg config.Enrichment) error {

	for _, enrichmentSet := range cfg.Sets {
		result, err := getCisByTrait(enrichmentSet, cfg)
		if err != nil {
			enrichmentsCache.RetryCount += 1

			if enrichmentsCache.RetryCount > cfg.RetryCount {
				enrichmentsCache.IsValid = false
			}

			return fmt.Errorf("Failed to get CIs for trait \"%s\": %w", enrichmentSet.TraitName, err)
		} else {
			enrichmentsCache.RetryCount = 0
			enrichmentsCache.IsValid = true
			enrichmentsCache.LastUpdate = time.Now()
		}

		updateEnrichmentCache(result, enrichmentSet)
	}

	return nil
}

func FindEnrichmentSetByName(name string, enrichmentSets []config.EnrichmentSet) (*config.EnrichmentSet, error) {
	for _, v := range enrichmentSets {
		if name == v.Name {
			return &v, nil
		}
	}

	err := fmt.Sprintf("The configured enrichmentset {%s} could not be found!", name)
	return nil, errors.New(err)
}

func updateEnrichmentCache(result map[string]okclient.EffectiveTraitDTO, enrichmentSet config.EnrichmentSet) {

	var enrichmentItems = map[string]map[string]string{}
	for _, value := range result {
		item := make(map[string]string)
		for traitAttributeIdentifier, v := range value.TraitAttributes {
			if !v.Value.IsArray {
				item[traitAttributeIdentifier] = v.Value.Values[0]
			} else {
				item[traitAttributeIdentifier] = strings.Join(v.Value.Values[:], ",")
			}
		}
		lookupAttribute := enrichmentSet.TraitAttributeIdentifier
		if _, ok := item[lookupAttribute]; !ok {
			continue // we cannot use a CI which does not contain the lookup attribute
		}
		lookupAttributeValue := item[lookupAttribute]

		// filter item map based on enrichmentSet.TraitAttributeList
		// this can also be used to delete the lookup attribute by not specifying it
		for k := range item {
			if !contains(enrichmentSet.TraitAttributeList, k) {
				delete(item, k)
			}
		}

		// TODO: how to deal with duplicate lookupAttribute values? For now we just override, so it's not deterministic which CI is used then
		enrichmentItems[lookupAttributeValue] = item
	}
	enrichmentsCache.CacheLock.Lock()
	enrichmentsCache.EnrichmentItems[enrichmentSet.Name] = enrichmentItems
	enrichmentsCache.CacheLock.Unlock()
}

func getCisByTrait(cfg config.EnrichmentSet, cfgFull config.Enrichment) (map[string]okclient.EffectiveTraitDTO, error) {

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

func EnrichTags(tags map[string]string, enrichmentSet *config.EnrichmentSet) (map[string]string, error) {
	if !enrichmentsCache.IsValid {
		return nil, errors.New("Failed to enrich metrics due to invalid enrichments cache!")
	}

	if lookupTagValue, ok := tags[enrichmentSet.LookupTag]; ok {
		var tagsCopy map[string]string = make(map[string]string)

		for k, v := range tags {
			tagsCopy[k] = v
		}

		enrichmentsCache.CacheLock.RLock()
		if traitAttributes, ok := enrichmentsCache.EnrichmentItems[enrichmentSet.Name]; ok {
			if attributes, ok := traitAttributes[lookupTagValue]; ok {
				for k, v := range attributes {
					tagsCopy[k] = v
				}
			}
		}
		enrichmentsCache.CacheLock.RUnlock()

		return tagsCopy, nil
	}

	// if there is nothing to enrich return the passed in tags
	return tags, nil
}

func ForceSetEnrichmentCache(enrichmentItems map[string]map[string]string, cacheEntryName string) {
	enrichmentsCache.CacheLock.Lock()
	enrichmentsCache.EnrichmentItems[cacheEntryName] = enrichmentItems
	enrichmentsCache.IsValid = true
	enrichmentsCache.LastUpdate = time.Now()
	enrichmentsCache.CacheLock.Unlock()
}

func GetEnrichmentCache() *Cache {
	return enrichmentsCache
}

var enrichmentsCache *Cache = &Cache{
	EnrichmentItems: map[string]map[string]map[string]string{},
	CacheLock:       sync.RWMutex{},
}

type Cache struct {
	EnrichmentItems map[string]map[string]map[string]string
	RetryCount      int
	LastUpdate      time.Time
	IsValid         bool
	CacheLock       sync.RWMutex
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
