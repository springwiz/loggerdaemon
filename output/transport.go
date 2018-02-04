package output

import "log"
import "github.com/spf13/viper"

// Config for Transport
type Transport struct {
	// AmqpHost to connect
	AmqpHost string

	// AmqpPort to connect
	AmqpPort string

	// Exchange name
	LogExchange string

	// User name
	User string

	// Password
	Password string
}

func NewTransport() Transport {
	// read the config yml
	viper.SetConfigName("server")
	viper.AddConfigPath("/Users/sumit/go")
	err := viper.ReadInConfig()
	host := "localhost"
	port := "5672"
	exchange := "logexchange"
	user := ""
	password := ""

	if err != nil {
		log.Println("Config file not found...")
		log.Println("Using Defaults")
	}

	host = viper.GetString("amqp.host")
	port = viper.GetString("amqp.port")
	exchange = viper.GetString("amqp.exchange")
	user = viper.GetString("amqp.user")
	password = viper.GetString("amqp.password")

	return Transport{
		AmqpHost:    host,
		AmqpPort:    port,
		LogExchange: exchange,
		User:        user,
		Password:    password,
	}
}
