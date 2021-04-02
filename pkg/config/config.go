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
	Port                           int
	LogLevel                       string            `json:"log_level"`
	InternalMetricsCollectInterval int               `json:"internal_metrics_collect_interval"`
	InternalMetricsFlushInterval   int               `json:"internal_metrics_flush_interval"`
	InternalMetricsMeasurement     string            `json:"internal_metrics_measurement"`
	OutputsTimescale               []OutputTimescale `json:"outputs_timescaledb"`
	OutputsInflux                  []OutputInflux    `json:"outputs_influxdb"`
}

type Tagfilter interface {
	GetTagfilterInclude() map[string][]string
	GetTagfilterBlock() map[string][]string
}

type OutputTimescale struct {
	TagfilterInclude map[string][]string             `json:"tagfilter_include"`
	TagfilterBlock   map[string][]string             `json:"tagfilter_block"`
	WriteStrategy    string                          `json:"write_strategy"`
	Measurements     map[string]MeasurementTimescale `json:"measurements"`
	Connection       string                          `json:"connection"`
}

func (c *OutputTimescale) GetTagfilterInclude() map[string][]string {
	return c.TagfilterInclude
}
func (c *OutputTimescale) GetTagfilterBlock() map[string][]string {
	return c.TagfilterBlock
}

type OutputInflux struct {
	TagfilterInclude map[string][]string          `json:"tagfilter_include"`
	TagfilterBlock   map[string][]string          `json:"tagfilter_block"`
	WriteStrategy    string                       `json:"write_strategy"`
	Measurements     map[string]MeasurementInflux `json:"measurements"`
	Connection       string                       `json:"connection"`
	DbName           string                       `json:"db_name"`
	Version          int                          `json:"version"`
	Org              string                       `json:"org"`
	AuthToken        string                       `json:"auth_token"`
	Username         string                       `json:"username"`
	Password         string                       `json:"password"`
}

func (c *OutputInflux) GetTagfilterInclude() map[string][]string {
	return c.TagfilterInclude
}
func (c *OutputInflux) GetTagfilterBlock() map[string][]string {
	return c.TagfilterBlock
}

type MeasurementTimescale struct {
	AddedTags       map[string]string
	FieldsAsColumns []string
	TagsAsColumns   []string
	TargetTable     string
	Ignore          bool
	IgnoreFiltering bool
}

type MeasurementInflux struct {
	AddedTags       map[string]string
	Ignore          bool
	IgnoreFiltering bool
}
