package src

import (
	"encoding/json"

	"github.com/nsqio/go-nsq"
)

type Consumer struct {
	Consumer    *nsq.Consumer
	messageChan chan *Message
}

func NewConsumer(topic, addr string) (*Consumer, error) {
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(topic, "", config)
	if err != nil {
		return nil, err //TODO: more comprehensive error scheme
	}

	c := &Consumer{
		messageChan: make(chan *Message),
	}
	consumer.AddHandler(c)

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

	c.messageChan <- message

	return nil
}

func (c *Consumer) ConsumeMessages(count int) {
}
