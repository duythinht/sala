package sala

import (
	"log"
	"os"
	"os/signal"

	cluster "github.com/bsm/sarama-cluster"
)

// Worker struct
type Worker struct {
	consumer *cluster.Consumer
	cfg      *Config
	apply    func(map[string]interface{})
}

// NewWorker return a sala worker
func NewWorker(c *Config, options *cluster.Config) *Worker {

	if options == nil {
		options = cluster.NewConfig()
	}

	consumer, err := cluster.NewConsumer(c.brokers, c.group, c.topics, options)

	fatalOrNext(err)

	defaultApply := func(m map[string]interface{}) {
		log.Printf("Receive message: %v", m)
	}

	return &Worker{
		consumer: consumer,
		cfg:      c,
		apply:    defaultApply,
	}
}

func (w *Worker) Bind(apply func(map[string]interface{})) {
	w.apply = apply
}

func (w *Worker) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	decoder := DefaultDecoder

	if w.cfg.Decoder != nil {
		decoder = w.cfg.Decoder
	}

	log.Printf("Start consume message")
	// consume messages, watch errors and notifications
	for {
		select {
		case msg, more := <-w.consumer.Messages():
			if more {
				if message, err := decoder.DecodeValue(msg.Value); err == nil {
					w.apply(message)
					w.consumer.MarkOffset(msg, "") // mark message as processed
				} else {
					log.Fatal(err)
				}
			}
		case err, more := <-w.consumer.Errors():
			if more {
				log.Printf("Error: %s\n", err.Error())
			}
		case ntf, more := <-w.consumer.Notifications():
			if more {
				log.Printf("Rebalanced: %+v\n", ntf)
			}
		case <-signals:
			return
		}
	}
}

func (w *Worker) Commit() {
	fatalOrNext(w.consumer.CommitOffsets())
}

func (w *Worker) Stop() {
	err := w.consumer.Close()
	fatalOrNext(err)
}
