package sala

import "strings"

type Config struct {
	brokers []string
	topics  []string
	group   string
	decoder Decoder
}

func NewConfig(addrs string, topics string, group string) *Config {
	brokers := strings.Split(addrs, ",")
	topicsList := strings.Split(topics, ",")
	return &Config{
		brokers: brokers,
		topics:  topicsList,
		group:   group,
		decoder: nil,
	}
}
