package main

import (
	"io"
	"net"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func TestLoggerDaemon(t *testing.T) {
	// read the config yml
	viper.SetConfigName("server")
	viper.AddConfigPath("..")
	err := viper.ReadInConfig()
	host := "localhost"
	port := "9999"
	protocol := "tcp"

	if err != nil {
		log.Infof("Config file not found...")
		log.Infof("Using Defaults")
	}

	host = viper.GetString("server.host")
	port = viper.GetString("server.port")
	protocol = viper.GetString("server.protocol")

	log.Infof("Run Host: %s", host)
	log.Infof("Run Port: %s", port)
	log.Infof("Run protocol: %s", protocol)

	// grab a tcp connection
	conn, err := net.Dial(protocol, host+":"+port)
	io.WriteString(conn, "Test String Msg")

	if err != nil {
		t.Error(err.Error())
	}

	defer conn.Close()
}
