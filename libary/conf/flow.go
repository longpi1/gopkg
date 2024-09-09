package conf

import (
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/longpi1/gopkg/libary/log"
	"github.com/spf13/viper"
)

var conf *Config
var once sync.Once

type FlowConfig struct {
	Name       string
	Deps       []string
	Definition string
}

type Config struct {
	Flows []FlowConfig
}

func GetFlowConfig(name string, filePath string) *Config {
	if conf == nil {
		// 初始化flow配置信息
		InitFlowConfig(name, filePath)
	}
	return conf
}

func InitFlowConfig(name string, path string) {
	// 解析 config
	viper.SetConfigName(name)
	viper.AddConfigPath(path)
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal("解析文件失败: ", err)
	}
	if err := viper.Unmarshal(&conf); err != nil {
		log.Fatal("解析文件失败: ", err)
	}
	// 监听配置更新
	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		if err := viper.Unmarshal(&conf); err != nil {
			log.Fatal("解析文件失败: ", err)
		}
	})
}
