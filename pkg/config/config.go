package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

func ReadConfigFromFile(configFile string, cfg *Configuration) error {
	file, err := os.Open(configFile)
	if err != nil {
		return fmt.Errorf("can't open config file: %w", err)
	}
	defer file.Close()

	byteValue, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("can't read config file: %w", err)
	}
	err = json.Unmarshal(byteValue, &cfg)
	if err != nil {
		return fmt.Errorf("can't parse config file: %w", err)
	}
	return nil
}

type Configuration struct {
	Port             int
	OutputsTimescale []OutputTimescale `json:"outputs_timescaledb"`
	OutputsInflux    []OutputInflux    `json:"outputs_influxdb"`
}

type OutputTimescale struct {
	TagfilterInclude map[string][]string                 `json:"tagfilter_include"`
	TagfilterBlock   map[string][]string                 `json:"tagfilter_block"`
	WriteStrategy    string                              `json:"write_strategy"`
	Measurements     map[string]MeasurementConfiguration `json:"measurements"`
	Connection       string                              `json:"connection"`
}

type OutputInflux struct {
	TagfilterInclude map[string][]string                 `json:"tagfilter_include"`
	TagfilterBlock   map[string][]string                 `json:"tagfilter_block"`
	WriteStrategy    string                              `json:"write_strategy"`
	Measurements     map[string]MeasurementConfiguration `json:"measurements"`
	Connection       string                              `json:"connection"`
	DbName           string                              `json:"db_name"`
	Org              string                              `json:"org"`
	AuthToken        string                              `json:"auth_token"`
}

type MeasurementConfiguration struct {
	AddedTags       map[string]string
	FieldsAsColumns []string
	TagsAsColumns   []string
	TargetTable     string
	Ignore          bool
}
