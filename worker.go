package sala

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	cluster "github.com/bsm/sarama-cluster"
)

type Worker struct {
	consumer *cluster.Consumer
	cfg      *Config
	apply    func(map[string]interface{})
}

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

	if w.cfg.decoder != nil {
		decoder = w.cfg.decoder
	}

	// consume messages, watch errors and notifications
	for {
		select {
		case msg, more := <-w.consumer.Messages():
			if more {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				if message, err := decoder.DecodeValue(msg.Value); err != nil {
					w.apply(message)
				}
				w.consumer.MarkOffset(msg, "") // mark message as processed
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
