package enrichments

import (
	"context"
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
			return err
		}

		updateEnrichmentCache(result, enrichmentSet.Name)
	}

	return nil
}

func updateEnrichmentCache(result map[string]okclient.EffectiveTraitDTO, setName string) {

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

	setEnrichmentsCacheValues(setName, enerichmentItems)
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

func EnrichMetrics(tags map[string]string, enrichmentSet config.EnrichmentSet) map[string]string {

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

	return tags
}

var enrichmentsCache *Cache = &Cache{
	EnrichmentItems: map[string][]map[string]string{},
	CacheLock:       sync.Mutex{},
}

type Cache struct {
	EnrichmentItems map[string][]map[string]string
	CacheLock       sync.Mutex
}

func setEnrichmentsCacheValues(key string, values []map[string]string) {
	enrichmentsCache.EnrichmentItems[key] = values
}
