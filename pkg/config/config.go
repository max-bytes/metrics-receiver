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
	InternalMetricsFlushCycle      int               `json:"internal_metrics_flush_cycle"`
	InternalMetricsMeasurement     string            `json:"internal_metrics_measurement"`
	Enrichment                     Enrichment        `json:"enrichment"`
	OutputsTimescale               []OutputTimescale `json:"outputs_timescaledb"`
	OutputsInflux                  []OutputInflux    `json:"outputs_influxdb"`
}

type OutputConfig interface {
	GetTagfilterInclude() map[string][]string
	GetTagfilterBlock() map[string][]string
	GetMeasurementConfig(name string) (MeasurementConfig, bool)
}

type MeasurementConfig interface {
	GetAddedTags() map[string]string
	GetIgnore() bool
	GetIgnoreFiltering() bool
	GetEnrichment() string
}

type OutputTimescale struct {
	TagfilterInclude map[string][]string             `json:"tagfilter_include"`
	TagfilterBlock   map[string][]string             `json:"tagfilter_block"`
	WriteStrategy    string                          `json:"write_strategy"`
	Measurements     map[string]MeasurementTimescale `json:"measurements"`
	Connection       string                          `json:"connection"`
}

func (c *OutputTimescale) GetTagfilterInclude() map[string][]string { return c.TagfilterInclude }
func (c *OutputTimescale) GetTagfilterBlock() map[string][]string   { return c.TagfilterBlock }
func (c *OutputTimescale) GetMeasurementConfig(name string) (MeasurementConfig, bool) {
	m, ok := c.Measurements[name]
	return m, ok
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

func (c *OutputInflux) GetTagfilterInclude() map[string][]string { return c.TagfilterInclude }
func (c *OutputInflux) GetTagfilterBlock() map[string][]string   { return c.TagfilterBlock }
func (c *OutputInflux) GetMeasurementConfig(name string) (MeasurementConfig, bool) {
	m, ok := c.Measurements[name]
	return m, ok
}

type MeasurementTimescale struct {
	AddedTags       map[string]string
	Ignore          bool
	IgnoreFiltering bool

	FieldsAsColumns []string
	TagsAsColumns   []string
	TargetTable     string

	Enrichment string
}

func (c MeasurementTimescale) GetAddedTags() map[string]string { return c.AddedTags }
func (c MeasurementTimescale) GetIgnore() bool                 { return c.Ignore }
func (c MeasurementTimescale) GetIgnoreFiltering() bool        { return c.IgnoreFiltering }
func (c MeasurementTimescale) GetEnrichment() string           { return c.Enrichment }

type MeasurementInflux struct {
	AddedTags       map[string]string
	Ignore          bool
	IgnoreFiltering bool

	Enrichment string
}

func (c MeasurementInflux) GetAddedTags() map[string]string { return c.AddedTags }
func (c MeasurementInflux) GetIgnore() bool                 { return c.Ignore }
func (c MeasurementInflux) GetIgnoreFiltering() bool        { return c.IgnoreFiltering }
func (c MeasurementInflux) GetEnrichment() string           { return c.Enrichment }

type Enrichment struct {
	Sets            []EnrichmentSet `json:"sets"`
	RetryCount      int             `json:"retry_count"`
	CollectInterval int             `json:"collect_interval"`
	ServerURL       string          `json:"server_url"`
	Username        string          `json:"username"`
	Password        string          `json:"password"`
	AuthURL         string          `json:"auth_url"`
	TokenURL        string          `json:"token_url"`
	ClientID        string          `json:"client_id"`
}

type EnrichmentSet struct {
	Name                     string   `json:"name"`
	TraitID                  string   `json:"trait_id"`
	TraitAttributeIdentifier string   `json:"trait_attribute_identifier"`
	TraitAttributeList       []string `json:"trait_attribute_list"`
	LayerIds                 []string `json:"layer_ids"`
	LookupTag                string   `json:"lookup_tag"`
	CaseInsensitiveMatching  bool     `json:"case_insensitive_matching"`
}
