package output

import (
	"strconv"

	"github.com/allegro/bigcache"
	log "github.com/sirupsen/logrus"
	"github.com/springwiz/loggerdaemon/common"
)

// Config for Logwriter
type Logwriter struct {
	// Transport
	LogTransport []common.Transport

	// Pointer to big cache
	LogCache *bigcache.BigCache

	// keyString
	Key string
}

func NewLogwriter(logCache *bigcache.BigCache, key string) Logwriter {
	return Logwriter{
		LogTransport: common.GetTransportSlice(),
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
			log.Println("Using existing publisher ")
		}
		if err != nil {
			log.Println("Error creating publisher: ", err)
			delete(publishMap, transInstance.Transport)
			return err
		}
		messageBody, err1 := l.LogCache.Get(l.Key)
		if err1 != nil {
			log.Println("The key ", l.Key, "not available in cache skipping")
		}
		l.LogCache.Set("SEQ"+strconv.Itoa(id)+strconv.FormatUint(seqNumber, 10), []byte(l.Key))
		log.Println("worker", id, "Set seqNumber key", seqNumber, l.Key)
		err2 := publisher.Publish(messageBody)
		if err2 != nil {
			log.Println("Error creating channel: ", err2)
			delete(publishMap, transInstance.Transport)
			return err2
		}
		log.Println("worker", id, "finished key", l.Key)
	}
	return nil
}
