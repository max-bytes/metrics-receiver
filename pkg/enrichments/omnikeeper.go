package enrichments

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/config"
)

func EnrichMetrics(cfg config.EnrichmentSet) {
	enrichments_err := getCiisByTrait(cfg)
	if enrichments_err != nil {
		logrus.Infof(enrichments_err.Error())
	}
}

func getCiisByTrait(cfg config.EnrichmentSet) error {
	url := cfg.BaseUrl + `/api/v1.0/CI/getAllCIIDs`
	resp, err := http.Get(url)
	if err != nil {
		return err
		// handle error
	}
	defer resp.Body.Close()
	byteValue, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("can't read config file: %w", err)
	}

	var result interface{}
	err = json.Unmarshal(byteValue, &result)
	if err != nil {
		return fmt.Errorf("can't parse config file: %w", err)
	}

	fmt.Println(result)
	return nil
}
