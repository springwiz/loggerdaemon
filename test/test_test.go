package test

import (
	"testing"
	"github.com/spf13/viper"
	"log"
	"net"
	"io"
)

func TestLoggerDaemon(t *testing.T) {
	// read the config yml
	viper.SetConfigName("server")
	viper.AddConfigPath("/Users/sumit/go")
	err := viper.ReadInConfig()
	host := "localhost"
	port := "9999"
	protocol := "tcp"

	if err != nil {
		log.Println("Config file not found...")
		log.Println("Using Defaults")
	}

	host = viper.GetString("server.host")
	port = viper.GetString("server.port")
	protocol = viper.GetString("server.protocol")

	t.Log("Run Host: ", host)
	t.Log("Run Port: ", port)
	t.Log("Run protocol: ", protocol)

	// grab a tcp connection
	conn, err := net.Dial(protocol, host+":"+port)
	io.WriteString(conn, "Test String Msg")

	if(err != nil) {
		t.Error(err.Error())
	}

	defer conn.Close()
}
