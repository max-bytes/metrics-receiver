package enrichments

import (
	"testing"

	"github.com/stretchr/testify/require"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
)

func TestBasicEnrichment(t *testing.T) {

	config := config.EnrichmentSet{
		Name:            "Test-Enrichmentset",
		TraitName:       "Test-Traitname",
		LayerIds:        []int64{0, 1, 2},
		LookupTag:       "Test-Lookuptag",
		LookupAttribute: "Test-Lookupattribute",
	}

	ForceSetEnrichmentCache([]map[string]string{
		{
			"Test-Lookupattribute": "123",
			"test":                 "foo",
		},
		{
			"Test-Lookupattribute": "456",
			"test":                 "bar",
		},
	}, config.Name)

	tagsBefore := map[string]string{
		"Test-Lookuptag": "123",
		"present-tag":    "present-tag-value",
	}
	tagsAfter, err := EnrichMetrics(tagsBefore, &config)

	require.Nil(t, err)
	require.Equal(t, map[string]string{
		"Test-Lookuptag": "123",
		"present-tag":    "present-tag-value",
		"test":           "foo",
	}, tagsAfter)
}
