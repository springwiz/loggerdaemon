package common

// publisher interface
// all publishers will implement this
type Publisher interface {
	Publish(messageBody []byte) error

	Cleanup()
}
