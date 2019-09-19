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
			log.Infof("Using existing publisher ")
		}
		if err != nil {
			log.Infof("Error creating publisher: %s", err)
			delete(publishMap, transInstance.Transport)
			return err
		}
		messageBody, err1 := l.LogCache.Get(l.Key)
		if err1 != nil {
			log.Infof("The key %s not available in cache skipping", l.Key)
		}
		_ = l.LogCache.Set("SEQ"+strconv.Itoa(id)+strconv.FormatUint(seqNumber, 10), []byte(l.Key))
		log.Infof("worker %d Set seqNumber key %d %s", id, seqNumber, l.Key)
		err2 := publisher.Publish(messageBody)
		if err2 != nil {
			log.Infof("Error creating channel: %s", err2)
			delete(publishMap, transInstance.Transport)
			return err2
		}
		log.Infof("worker %d finished key %s", id, l.Key)
	}
	return nil
}
