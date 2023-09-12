package src

import (
	"encoding/json"

	"github.com/nsqio/go-nsq"
)

type Publisher struct {
	producer *nsq.Producer
}

func NewPublisher(addr string) (*Publisher, error) {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		return nil, err //TODO: more comprehensive error scheme
	}

	return &Publisher{
		producer: producer,
	}, nil
}

func (p *Publisher) PublishMessages(messages ...*Message) error {
	for _, m := range messages {
		payload, err := json.Marshal(m.Payload)
		if err != nil {
			return err //TODO: more comprehensive error scheme
		}

		if err := p.producer.Publish(m.Topic, payload); err != nil {
			return err
		}
	}

	return nil
}
