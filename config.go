package sala

import "strings"

type Config struct {
	brokers []string
	topics  []string
	group   string
	Decoder Decoder
}

// NewConfig create a sala config, which pass by kafka addrs, topics and group name
func NewConfig(addrs string, topics string, group string) *Config {
	brokers := strings.Split(addrs, ",")
	topicsList := strings.Split(topics, ",")
	return &Config{
		brokers: brokers,
		topics:  topicsList,
		group:   group,
		Decoder: nil,
	}
}
