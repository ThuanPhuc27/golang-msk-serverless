package config

import (
	"os"
	"gopkg.in/yaml.v2"
)

type MSKConfig struct {
	Brokers      string `yaml:"brokers"`
	Topic        string `yaml:"topic"`
	ConsumerGroup string `yaml:"consumer_group"`
}

type AWSConfig struct {
	Region string `yaml:"region"`
}

type AppConfig struct {
	MSK MSKConfig `yaml:"msk"`
	AWS AWSConfig `yaml:"aws"`
}

func LoadConfig(path string) (*AppConfig, error) {
	config := &AppConfig{}
	
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	
	err = yaml.Unmarshal(file, config)
	if err != nil {
		return nil, err
	}
	
	return config, nil
}