package output

import "log"
import "fmt"
import "os"
import "github.com/streadway/amqp"
import "github.com/allegro/bigcache"
import "strconv"
import "strings"
import "github.com/springwiz/loggerdaemon/common"

// Config for AmqpPublisher
type AmqpPublisher struct {
	// Transport used
	LogTransport common.Transport

	// amqp uri
	Uri string

	// Logger
	Logger *log.Logger

	// Amqp Channel
	AmqpChannel *amqp.Channel

	// Pointer to big cache
	LogCache *bigcache.BigCache

	// ThreadId
	ThreadId int

	// AmqpConnection
	amqpConnection *amqp.Connection
}

func createPublisher(logTransport common.Transport, logCache *bigcache.BigCache, threadId int) (common.Publisher, error) {
	var publisher common.Publisher
	var err error
	if strings.Contains(logTransport.Transport, "amqp") {
		publisher, err = newAmqpPublisher(logTransport, logCache, threadId)
	} else if strings.Contains(logTransport.Transport, "kafka") {
		// TODO: write support code for kafka
	} else {
		// TODO: write support code for jms
	}
	return publisher, err
}

func newAmqpPublisher(logTransport common.Transport, logCache *bigcache.BigCache, threadId int) (AmqpPublisher, error) {
	var urlSecurity string
	if logTransport.User != "" && logTransport.Password != "" {
		urlSecurity = logTransport.User + ":" + logTransport.Password + "@"
	} else {
		urlSecurity = ""
	}
	uri := logTransport.Transport + "://" + urlSecurity + logTransport.Host + ":" + logTransport.Port + "/"
	log.Printf("dialing %q", uri)

	// grab an amqp connection
	conn, err := amqp.Dial(uri)
	if err != nil {
		return AmqpPublisher{}, fmt.Errorf("Dial: %s", err)
	}

	channel, err1 := conn.Channel()
	if err1 != nil {
		return AmqpPublisher{}, fmt.Errorf("Channel: %s", err1)
	}
	log.Printf("got Channel, declaring %q Exchange (%q)", "fanout", logTransport.LogExchange)
	if err2 := channel.ExchangeDeclare(
		logTransport.LogExchange, // name
		"fanout",                 // type
		true,                     // durable
		false,                    // auto-deleted
		false,                    // internal
		false,                    // noWait
		nil,                      // arguments
	); err2 != nil {
		return AmqpPublisher{}, fmt.Errorf("Exchange Declare: %s", err2)
	}

	return AmqpPublisher{
		LogTransport:   logTransport,
		Uri:            uri,
		Logger:         log.New(os.Stdout, "AmqpPublisher", log.Ldate|log.Ltime),
		AmqpChannel:    channel,
		LogCache:       logCache,
		ThreadId:       threadId,
		amqpConnection: conn,
	}, nil
}

func (p AmqpPublisher) Publish(messageBody []byte) error {
	p.Logger.Printf("enabling publishing confirms.")
	if err := p.AmqpChannel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}
	confirms := p.AmqpChannel.NotifyPublish(make(chan amqp.Confirmation, 100))
	log.Println("declared Exchange, publishing body: ", len(messageBody))

	defer p.confirmOne(confirms)

	if err := p.AmqpChannel.Publish(
		p.LogTransport.LogExchange, // publish to an exchange
		"ignore",                   // routing to 0 or more queues
		false,                      // mandatory
		false,                      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/x-protobuf",
			ContentEncoding: "",
			Body:            messageBody,
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	); err != nil {
		return fmt.Errorf("Exchange Publish: %s", err)
	}
	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func (p AmqpPublisher) confirmOne(confirms <-chan amqp.Confirmation) {
	p.Logger.Printf("waiting for confirmation of one publishing")
	confirmed := <-confirms

	// find the key in the cache using the ack no
	p.Logger.Println("Got id seqNumber", p.ThreadId, confirmed.DeliveryTag)
	keyBytes, err := p.LogCache.Get("SEQ" + strconv.Itoa(p.ThreadId) + strconv.FormatUint(confirmed.DeliveryTag, 10))
	if err != nil {
		p.Logger.Printf("The key (%s) not available in cache skipping", string(keyBytes))
	}

	if confirmed.Ack {
		// ack received clean up the log cache
		p.Logger.Printf("successful delivery of delivery tag: %d", confirmed.DeliveryTag)
		p.LogCache.Delete("SEQ" + strconv.Itoa(p.ThreadId) + strconv.FormatUint(confirmed.DeliveryTag, 10))
		p.LogCache.Delete(string(keyBytes))
	} else {
		// retry the msg not ack from rabbit mq
		p.Logger.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

// cleanup
func (p AmqpPublisher) Cleanup() {
	p.amqpConnection.Close()
}
