package output

import "log"
import "os"
import "github.com/allegro/bigcache"
import "strconv"
import "github.com/springwiz/loggerdaemon/common"

// Config for Logwriter
type Logwriter struct {
	// Transport
	LogTransport []common.Transport

	// Logger
	Logger *log.Logger

	// Pointer to big cache
	LogCache *bigcache.BigCache

	// keyString
	Key string
}

func NewLogwriter(logCache *bigcache.BigCache, key string) Logwriter {
	return Logwriter{
		LogTransport: common.GetTransportSlice(),
		Logger:       log.New(os.Stdout, "Logwriter", log.Ldate|log.Ltime),
		LogCache:     logCache,
		Key:          key,
	}
}

// Here's the worker, of which we'll run several
// concurrent instances. These workers will receive
// messages on the `messages` channel and write the messages to an amqp client
func (l Logwriter) DoWork(id int, publishMap map[string]common.Publisher, seqNumber uint64) error {
	var publisher common.Publisher
	var err error
	for _, transInstance := range l.LogTransport {
		if publishMap[transInstance.Transport] == nil {
			publisher, err = createPublisher(transInstance, l.LogCache, id)
			publishMap[transInstance.Transport] = publisher
		} else {
			publisher = publishMap[transInstance.Transport]
			l.Logger.Println("Using existing publisher ")
		}
		if err != nil {
			l.Logger.Println("Error creating publisher: ", err)
			delete(publishMap, transInstance.Transport)
			return err
		}
		messageBody, err1 := l.LogCache.Get(l.Key)
		if err1 != nil {
			l.Logger.Println("The key ", l.Key, "not available in cache skipping")
		}
		l.LogCache.Set("SEQ"+strconv.Itoa(id)+strconv.FormatUint(seqNumber, 10), []byte(l.Key))
		l.Logger.Println("worker", id, "Set seqNumber key", seqNumber, l.Key)
		err2 := publisher.Publish(messageBody)
		if err2 != nil {
			l.Logger.Println("Error creating channel: ", err2)
			delete(publishMap, transInstance.Transport)
			return err2
		}
		l.Logger.Println("worker", id, "finished key", l.Key)
	}
	return nil
}
