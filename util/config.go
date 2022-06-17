package util

import (
	"log"

	"github.com/spf13/viper"
)

type ViperConfig struct {
	RedisEndpoint string `mapstructure:"REDIS_ENDPOINT"`
	RedisWorkers  int    `mapstructure:"REDIS_WORKERS"`
	BatchTimeout  int64  `mapstructure:"BATCH_TIMEOUT"`
	BatchMaxSize  int    `mapstructure:"BATCH_MAX_SIZE"`
	TradeSymbols  string `mapstructure:"TRADE_SYMBOLS"`
	BarSymbols    string `mapstructure:"BAR_SYMBOLS"`
	StatusSymbols string `mapstructure:"STATUS_SYMBOLS"`
	QuoteSymbols  string `mapstructure:"QUOTE_SYMBOLS"`
}

var Config ViperConfig

func init() {

	config, err := LoadConfig(".")
	if err != nil {
		log.Fatal("cannot load configuration", err)
	}
	Config = config
}

// LoadConfig loads app.env if it exists and sets envars
func LoadConfig(path string) (config ViperConfig, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}
