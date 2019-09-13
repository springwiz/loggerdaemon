package main

import (
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache"
	log "github.com/sirupsen/logrus"
	"github.com/springwiz/loggerdaemon/output"
	"github.com/springwiz/loggerdaemon/workpool"
)

// Config for Input
type Input struct {
	// Host to connect
	Host string

	// Port to connect
	Port string

	// Protocol used
	Protocol string

	// Pointer to big cache
	LogCache *bigcache.BigCache

	// flag
	terminate bool

	// Key Indexes
	lastReceivedKey uint64

	lastSubmittedKey uint64

	// Lock Mutex
	lastlastReceivedMutex sync.Mutex
}

var cache *bigcache.BigCache

func New(host string, port string, protocol string, logCache *bigcache.BigCache) *Input {
	cache = logCache
	return &Input{
		Host:             host,
		Port:             port,
		Protocol:         protocol,
		LogCache:         logCache,
		lastReceivedKey:  0,
		lastSubmittedKey: 0,
	}
}

// polls the socket connection to pull data.
func (i *Input) Run() error {
	readBuffer := make([]byte, 4096)
	finalBytes := make([]byte, 0)
	i.terminate = false

	log.Println("Run Host: ", i.Host)
	log.Println("Run Port: ", i.Port)
	log.Println("Run protocol: ", i.Protocol)

	server, err := net.Listen(i.Protocol, i.Host+":"+i.Port)
	if err != nil {
		log.Println("Error listetning: ", err)
		os.Exit(1)
	}
	defer server.Close()
	log.Println("Server started! Waiting for connections...")

	// create workpool
	workpool.PoolWorkers = workpool.NewWorkpool(10)

	// start the poller routine
	go i.pollCache()

	for {
		connection, err := server.Accept()
		if err != nil {
			log.Println("Error: ", err)
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
				log.Println("Receiving data: ", eofError.Error())
				break
			} else {
				log.Println("Receiving data Bytes read: ", len)
			}
		}

		// push the data into bigcache
		i.lastlastReceivedMutex.Lock()
		i.lastReceivedKey += 1
		keyString := strconv.FormatUint(i.lastReceivedKey, 10)
		i.LogCache.Set(keyString, finalBytes)
		i.lastlastReceivedMutex.Unlock()

		// empty buffer
		readBuffer = make([]byte, 4096)
		finalBytes = make([]byte, 0)
	}
	i.terminate = true
	return nil
}

// polls the cache
func (i *Input) pollCache() {
	var counterKey uint64
	for {
		i.lastlastReceivedMutex.Lock()
		counterKey = i.lastReceivedKey
		i.lastlastReceivedMutex.Unlock()
		if i.terminate {
			log.Println("terminate")
			break
		} else if workpool.PoolWorkers.Hold {
			time.Sleep(2 * 60 * 1000000000)
			workpool.PoolWorkers.Hold = false
		} else if counterKey > i.lastSubmittedKey {
			log.Println("received submitted cache_size: ", i.lastReceivedKey, i.lastSubmittedKey, i.LogCache.Len())

			// add the LogWriter to the worker pool
			for i.lastSubmittedKey < counterKey {
				var worker workpool.Worker
				atomic.AddUint64(&i.lastSubmittedKey, 1)
				worker = output.NewLogwriter(i.LogCache, strconv.FormatUint(i.lastSubmittedKey, 10))
				workpool.PoolWorkers.AddTask(worker)
			}
		}
		time.Sleep(1 * 1000000000)
	}
	defer workpool.PoolWorkers.Shutdown()
}

// retry the expiring key
func RetryKey(key string, entry []byte) {
	if strings.Contains(key, "SEQ") {
		// its a sequence number
		// delete the cache entry
		cache.Delete(key)
	} else {
		// retry the key
		var worker workpool.Worker
		worker = output.NewLogwriter(cache, key)
		workpool.PoolWorkers.AddTask(worker)
	}
}
