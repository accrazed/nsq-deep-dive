package src

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/nsqio/go-nsq"
)

type Consumer struct {
	consumer *nsq.Consumer
	r        *rand.Rand
}

func NewConsumer(topic, addr string, numHandlers int) (*Consumer, error) {
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(topic, "deep-dive-channel", config)
	if err != nil {
		return nil, err //TODO: more comprehensive error scheme
	}

	c := &Consumer{
		consumer: consumer,
		r:        rand.New(rand.NewSource(time.Now().Unix())),
	}
	consumer.AddConcurrentHandlers(c, numHandlers)

	if err := consumer.ConnectToNSQLookupd(addr); err != nil {
		return nil, err //TODO: more comprehensive error scheme
	}

	return c, nil
}

func (c *Consumer) HandleMessage(m *nsq.Message) error {
	messagePay := &MessagePayload{}

	if err := json.Unmarshal(m.Body, &messagePay); err != nil {
		return err //TODO: more comprehensive error scheme
	}

	// Random chance to throw error
	if c.r.Int()%5 == 0 {
		return fmt.Errorf("error when handling message: ( %s : %d )",
			messagePay.Body, messagePay.Status)
	}

	// "Process" the message
	fmt.Printf("New Message!\nMessage: %s\nStatus: %d\n\n",
		messagePay.Body, messagePay.Status)

	return nil
}

func (c *Consumer) Stop() {
	c.consumer.Stop()
}

func (c *Consumer) StopChan() chan int {
	return c.consumer.StopChan
}
