package worker

import (
	"encoding/json"
	"io/ioutil"
)

// 程序配置
type Config struct {
	EtcdEndpoints   []string `json:"etcdEndpoints"`
	EtcdDialTimeout int      `json:"etcdDialTimeout"`
}

var (
	G_config *Config
)

func InitConfig(filename string) error {
	var (
		content []byte
		conf    Config
		err     error
	)
	// 加载json配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return err
	}

	// json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return err
	}

	//单例赋值
	G_config = &conf

	return nil
}
