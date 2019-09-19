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
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	var host, port, protocol string
	if err != nil {
		log.Infof("Config file not found...")
		log.Infof("Using Defaults")
		host = "localhost"
		port = "9999"
		protocol = "tcp"
	} else {
		host = viper.GetString("server.host")
		port = viper.GetString("server.port")
		protocol = viper.GetString("server.protocol")
	}
	log.Infof("Run Host: %s", host)
	log.Infof("Run Port: %s", port)
	log.Infof("Run protocol: %s", protocol)

	// grab a tcp connection
	var conn net.Conn
	conn, err = net.Dial(protocol, host+":"+port)
	if err != nil {
		t.Error(err.Error())
	}
	_, err = io.WriteString(conn, "Test String Msg")
	if err != nil {
		t.Error(err.Error())
	}
	defer conn.Close()
}
