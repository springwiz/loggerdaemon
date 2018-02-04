package output

import "log"
import "fmt"
import "os"
import "github.com/streadway/amqp"
import "github.com/allegro/bigcache"
import "strconv"

// Config for Publisher
type Publisher struct {
	// Transport used
	LogTransport Transport

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
}

var (
	AmqpConnection *amqp.Connection
	RetryChannel   chan string = make(chan string, 100)
)

func New(logTransport Transport, logCache *bigcache.BigCache, threadId int) (Publisher, error) {
	var urlSecurity string
	if logTransport.User != "" && logTransport.Password != "" {
		urlSecurity = logTransport.User + ":" + logTransport.Password + "@"
	} else {
		urlSecurity = ""
	}
	uri := "amqp://" + urlSecurity + logTransport.AmqpHost + ":" + logTransport.AmqpPort + "/"
	log.Printf("dialing %q", uri)

	// grab an amqp connection
	conn, err := amqp.Dial(uri)
	AmqpConnection = conn
	if err != nil {
		return Publisher{}, fmt.Errorf("Dial: %s", err)
	}

	channel, err1 := AmqpConnection.Channel()
	if err1 != nil {
		return Publisher{}, fmt.Errorf("Channel: %s", err1)
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
		return Publisher{}, fmt.Errorf("Exchange Declare: %s", err2)
	}

	return Publisher{
		LogTransport: logTransport,
		Uri:          uri,
		Logger:       log.New(os.Stdout, "Publisher", log.Ldate|log.Ltime),
		AmqpChannel:  channel,
		LogCache:     logCache,
		ThreadId:     threadId,
	}, nil
}

func (p Publisher) Publish(messageBody []byte) error {
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
func (p Publisher) confirmOne(confirms <-chan amqp.Confirmation) {
	p.Logger.Printf("waiting for confirmation of one publishing")
	confirmed := <-confirms

	// find the key in the cache using the ack no
	keyBytes, err := p.LogCache.Get(strconv.Itoa(p.ThreadId) + strconv.FormatUint(confirmed.DeliveryTag, 10))
	if err != nil {
		p.Logger.Printf("The key (%d) not available in cache skipping", string(keyBytes))
	}

	if confirmed.Ack {
		// ack received clean up the log cache
		p.Logger.Printf("successful delivery of delivery tag: %d", confirmed.DeliveryTag)
		p.LogCache.Delete(string(keyBytes))
	} else {
		// retry the msg not ack from rabbit mq
		p.Logger.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		RetryChannel <- string(keyBytes)
	}
}
