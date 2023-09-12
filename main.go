package main

import (
	"nsq-deep-dive/src"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	addr, ok := os.LookupEnv("PUBLISHER_ADDR")
	if !ok {
		panic("PUBLISHER_ADDR not set")
	}

	pub, err := src.NewPublisher(addr)
	if err != nil {
		panic(err)
	}

	msgs := []*src.Message{
		{
			Topic: "First Topic",
			Payload: &src.MessagePayload{
				Status: 200,
				Body:   "Hello!! I'm Amelia!",
			},
		},
		{
			Topic: "Second Topic",
			Payload: &src.MessagePayload{
				Status: 201,
				Body:   "Hello!! I'm Liz! I sent a 201",
			},
		},
		{
			Topic: "Third Topic",
			Payload: &src.MessagePayload{
				Status: 404,
				Body:   "Hello!! I'm a 404!",
			},
		},
		{
			Topic: "First Topic",
			Payload: &src.MessagePayload{
				Status: 200,
				Body:   "Hello Amelia!! I'm also Amelia!",
			},
		},
	}
	if err := pub.PublishMessages(msgs...); err != nil {
		panic(err)
	}
}
