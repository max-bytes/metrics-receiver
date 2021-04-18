package enrichments

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
	okclient "www.mhx.at/gitlab/landscape/omnikeeper-client-go.git"
)

var retryCount = 0
var api_client *okclient.APIClient
var auth context.Context
var apiVersion = "1"

func init() {
	username := "omnikeeper-client-library-test"
	password := "omnikeeper-client-library-test"
	serverURL := "https://acme.omnikeeper-dev.bymhx.at/backend"

	oauth2cfg := &oauth2.Config{
		ClientID: "landscape-omnikeeper",
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://auth-dev.mhx.at/auth/realms/acme/protocol/openid-connect/auth",
			TokenURL: "https://auth-dev.mhx.at/auth/realms/acme/protocol/openid-connect/token",
		},
	}

	ctx := context.Background()
	token, err := oauth2cfg.PasswordCredentialsToken(ctx, username, password)
	exitOnError(err)

	configuration := okclient.NewConfiguration()
	configuration.Servers[0].URL = serverURL
	api_client = okclient.NewAPIClient(configuration)

	tokenSource := oauth2cfg.TokenSource(ctx, token)
	auth = context.WithValue(ctx, okclient.ContextOAuth2, tokenSource)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func EnrichMetrics(cfg config.EnrichmentSets) {
	for _, enrichmentSet := range cfg.Sets {
		getCisByTraitV2(enrichmentSet)
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
}

func getCisByTraitV2(cfg config.EnrichmentSet) (map[string]Trait, error) {

	resp, r, err := api_client.TraitApi.GetEffectiveTraitsForTraitName(auth, apiVersion).LayerIDs(cfg.LayerIds).TraitName(cfg.TraitName).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `TraitApi.GetEffectiveTraitsForTraitName``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}

	fmt.Fprintf(os.Stdout, "Response from `TraitApi.GetEffectiveTraitsForTraitName`: %v\n", resp)

	return nil, nil
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

func intListToStringList(a []int64) []string {
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
