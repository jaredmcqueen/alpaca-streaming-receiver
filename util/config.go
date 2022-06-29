package util

import (
	"log"

	"github.com/spf13/viper"
)

type Envars struct {
	RedisBatchMaxSize int    `mapstructure:"REDIS_BATCH_MAX_SIZE"`
	RedisBatchTimeout int64  `mapstructure:"REDIS_BATCH_TIMEOUT"`
	RedisEndpoint     string `mapstructure:"REDIS_ENDPOINT"`
	RedisWorkers      int    `mapstructure:"REDIS_WORKERS"`

	SymbolsBars     string `mapstructure:"SYMBOLS_BARS"`
	SymbolsQuotes   string `mapstructure:"SYMBOLS_QUOTES"`
	SymbolsStatuses string `mapstructure:"SYMBOLS_STATUSES"`
	SymbolsTrades   string `mapstructure:"SYMBOLS_TRADES"`

	EnableBars     bool `mapstructure:"ENABLE_BARS"`
	EnableQuotes   bool `mapstructure:"ENABLE_QUOTES"`
	EnableStatuses bool `mapstructure:"ENABLE_STATUSES"`
	EnableTrades   bool `mapstructure:"ENABLE_TRADES"`

	ChannelQueueSize int `mapstructure:"CHANNEL_QUEUE_SIZE"`
}

var Config Envars

func init() {
	config, err := loadConfig(".")
	if err != nil {
		log.Fatal("cannot load configuration", err)
	}
	Config = config
}

// loadConfig loads app.env if it exists and sets envars
func loadConfig(path string) (envars Envars, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&envars)
	return
}
