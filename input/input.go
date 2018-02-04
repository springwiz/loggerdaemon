package input

import "log"
import "os"
import "net"
import "io"
import "github.com/allegro/bigcache"
import "time"
import "github.com/springwiz/loggerdaemon/workpool"
import "github.com/springwiz/loggerdaemon/output"
import "strconv"

// Config for Input
type Input struct {
	// Host to connect
	Host string

	// Port to connect
	Port string

	// Protocol used
	Protocol string

	// Logger
	Logger *log.Logger

	// Pointer to big cache
	LogCache *bigcache.BigCache
}

func New(host string, port string, protocol string, logCache *bigcache.BigCache) Input {
	return Input{
		Host:     host,
		Port:     port,
		Protocol: protocol,
		Logger:   log.New(os.Stdout, "Input", log.Ldate|log.Ltime),
		LogCache: logCache,
	}
}

// polls the socket connection to pull data.
func (i Input) Run() error {
	readBuffer := make([]byte, 4096)
	finalBytes := make([]byte, 0)

	i.Logger.Println("Run Host: ", i.Host)
	i.Logger.Println("Run Port: ", i.Port)
	i.Logger.Println("Run protocol: ", i.Protocol)

	server, err := net.Listen(i.Protocol, i.Host+":"+i.Port)
	if err != nil {
		i.Logger.Println("Error listetning: ", err)
		os.Exit(1)
	}
	defer server.Close()
	i.Logger.Println("Server started! Waiting for connections...")

	// channel for sending out the messages to workers
	messages := make(chan string, 1000)

	// create worker threads for handling the messages
	// This starts up 10 workers, initially blocked
	poolWorkers := workpool.NewWorkpool(i.LogCache)
	for w := 1; w <= 10; w++ {
		go poolWorkers.DoWork(w, messages)
	}

	// thread for retry channel
	go poolWorkers.DoWork(11, output.RetryChannel)

	for {
		connection, err := server.Accept()
		if err != nil {
			i.Logger.Println("Error: ", err)
			os.Exit(1)
		}
		defer connection.Close()

		// read all bytes from the connection
		// append into a byte slice
		len := 0
		var eofError error
		for {
			len, eofError = io.ReadFull(connection, readBuffer)
			finalBytes = append(finalBytes, readBuffer[0:len]...)
			if eofError == io.EOF || eofError == io.ErrUnexpectedEOF {
				i.Logger.Println("EOF encountered reading from the Server Socket: ", err)
				break
			} else {
				i.Logger.Println("No of bytes read sucessfully: ", len)
			}
		}

		// push the data into bigcache
		key :=  time.Now().UnixNano()
		keyString := strconv.FormatInt(key, 10)
		i.LogCache.Set(keyString, finalBytes)

		// empty buffer
		readBuffer = make([]byte, 4096)
		finalBytes = make([]byte, 0)

		// pass the key and cache reference to worker
		messages <- keyString
	}

	close(messages)
	close(output.RetryChannel)
	output.AmqpConnection.Close()
	return nil
}
