package enrichments

import (
	"testing"

	"github.com/max-bytes/metrics-receiver/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestBasicEnrichment(t *testing.T) {

	config := config.EnrichmentSet{
		Name:                     "Test-Enrichmentset",
		TraitID:                  "Test-Traitname",
		LayerIds:                 []string{"0", "1", "2"},
		LookupTag:                "Test-Lookuptag",
		TraitAttributeIdentifier: "Test-Lookupattribute",
		TraitAttributeList:       []string{"test"},
	}

	ForceSetEnrichmentCache(map[string]map[string]string{
		"123": {
			"test": "foo",
		},
		"456": {
			"test": "bar",
		},
	}, config.Name)

	tagsBefore := map[string]string{
		"Test-Lookuptag": "123",
		"present-tag":    "present-tag-value",
	}
	tagsAfter, err := EnrichTags(tagsBefore, &config)

	require.Nil(t, err)
	require.Equal(t, map[string]string{
		"Test-Lookuptag": "123",
		"present-tag":    "present-tag-value",
		"test":           "foo",
	}, tagsAfter)
}
