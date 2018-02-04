package workpool

import "log"
import "os"
import "github.com/allegro/bigcache"
import "github.com/springwiz/loggerdaemon/output"
import "strconv"

// Config for Workpool
type Workpool struct {
	// Transport
	LogTransport output.Transport

	// Logger
	Logger *log.Logger

	// Pointer to big cache
	LogCache *bigcache.BigCache
}

func NewWorkpool(logCache *bigcache.BigCache) Workpool {
	return Workpool{
		LogTransport: output.NewTransport(),
		Logger:       log.New(os.Stdout, "Workpool", log.Ldate|log.Ltime),
		LogCache:     logCache,
	}
}

// Here's the worker, of which we'll run several
// concurrent instances. These workers will receive
// messages on the `messages` channel and write the messages to an amqp client
func (w Workpool) DoWork(id int, messages <-chan string) {
	publisher, err := output.New(w.LogTransport, w.LogCache, id)
	if err != nil {
		w.Logger.Println("Error creating publisher: ", err)
		os.Exit(1)
	}
	var seqNumber uint64
	seqNumber = 1
	for j := range messages {
		w.Logger.Println("worker", id, "started  job", j)
		messageBody, err1 := w.LogCache.Get(j)
		if err1 != nil {
			w.Logger.Printf("The key (%d) not available in cache skipping", j)
		}
		w.LogCache.Set(strconv.Itoa(id)+strconv.FormatUint(seqNumber, 10), []byte(j))
		publisher.Publish(messageBody)
		seqNumber++
		w.Logger.Println("worker", id, "finished job", j)
	}
}
