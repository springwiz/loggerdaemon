package common

import (
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Config for Transport
type Transport struct {
	// transport type
	Transport string

	// Host to connect
	Host string

	// Port to connect
	Port string

	// Exchange name
	LogExchange string

	// User name
	User string

	// Password
	Password string
}

func GetTransportSlice() []Transport {
	// read the config yml
	viper.SetConfigName("server")
	viper.AddConfigPath("/Users/sumit/go")
	err := viper.ReadInConfig()
	transSlice := make([]Transport, 0)

	if err != nil {
		log.Warnf("Config file not found...")
		log.Warnf("Using Defaults")
		transSlice = append(transSlice, Transport{"amqp", "localhost", "5672", "logexchange", "", ""})
		return transSlice
	}

	outputInterface := viper.Get("output")
	outputSlice := outputInterface.([]interface{})
	for _, output := range outputSlice {
		outputMap := output.(map[interface{}]interface{})
		transport := new(Transport)
		transport.Host = outputMap["host"].(string)
		transport.LogExchange = outputMap["exchange"].(string)
		if outputMap["password"] != nil {
			transport.Password = outputMap["password"].(string)
		}
		transport.Port = strconv.Itoa(outputMap["port"].(int))
		transport.Transport = outputMap["transport"].(string)
		if outputMap["user"] != nil {
			transport.User = outputMap["user"].(string)
		}
		transSlice = append(transSlice, *transport)
	}
	return transSlice
}

func NewTransport() Transport {
	// read the config yml
	viper.SetConfigName("server")
	viper.AddConfigPath("..")
	err := viper.ReadInConfig()
	var transport, host, port, exchange, user, password string

	if err != nil {
		log.Warnf("Config file not found...")
		log.Warnf("Using Defaults")
		transport = "amqp"
		host = "localhost"
		port = "5672"
		exchange = "logexchange"
		user = ""
		password = ""
	} else {
		transport = viper.GetString("output.transport")
		host = viper.GetString("output.host")
		port = viper.GetString("output.port")
		exchange = viper.GetString("output.exchange")
		user = viper.GetString("output.user")
		password = viper.GetString("output.password")
	}

	return Transport{
		Transport:   transport,
		Host:        host,
		Port:        port,
		LogExchange: exchange,
		User:        user,
		Password:    password,
	}
}
