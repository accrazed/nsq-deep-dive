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
	consumer, err := nsq.NewConsumer(topic, "", config)
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
	message := &Message{}

	if err := json.Unmarshal(m.Body, message); err != nil {
		return err //TODO: more comprehensive error scheme
	}

	// Random chance to throw error
	if c.r.Int()%2 == 0 {
		return fmt.Errorf("random error when handling message: %#v", m)
	}

	// "Process" the message
	fmt.Printf("New Message!\nTopic: %s\nMessage: %s\nStatus: %d\n",
		message.Topic, message.Payload.Body, message.Payload.Status)

	return nil
}

func (c *Consumer) Stop() {
	c.consumer.Stop()
}

func (c *Consumer) StopChan() chan int {
	return c.consumer.StopChan
}
