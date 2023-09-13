package cli

import (
	"fmt"
	"nsq-deep-dive/src"
	"os"

	"github.com/urfave/cli/v2"
)

func RunCLI() {
	app := &cli.App{
		Name:                      "NSQ Deep Dive",
		Description:               "Publish and Consume messages on your local NSQ cluster",
		DisableSliceFlagSeparator: true,
		Commands: []*cli.Command{
			{
				Name:    "publish",
				Aliases: []string{"pub"},
				Usage:   "Publish one or more messages to the cluster",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "address",
						Aliases:  []string{"a"},
						Usage:    "The NSQ TCP pubisher address",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "topic",
						Aliases:  []string{"t"},
						Usage:    "the topic to send the messages to",
						Required: true,
					},
					&cli.StringSliceFlag{
						Name:     "messages",
						Aliases:  []string{"m"},
						Usage:    "the messages you'd like to send. linked sequentially to statuses",
						Required: true,
					},
					&cli.IntSliceFlag{
						Name:     "statuses",
						Aliases:  []string{"s"},
						Usage:    "the statuses for each message you send. linked sequentially to messages",
						Required: true,
					},
				},
				Action: DoPublish,
			},
			{
				Name:    "consume",
				Aliases: []string{"con"},
				Usage:   "consume a continuous stream of messages from a cluster topic",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "address",
						Aliases:  []string{"a"},
						Usage:    "The NSQ TCP consumer address",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "topic",
						Aliases: []string{"t"},
						Usage:   "the topic to consume messages from",

						Required: true,
					},
					&cli.UintFlag{
						Name:     "handlers",
						Aliases:  []string{"n"},
						Usage:    "the number of concurrent handlers to create for consumption",
						Required: true,
					},
				},
				Action: DoConsume,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func DoPublish(ctx *cli.Context) error {
	address := ctx.String("address")
	topic := ctx.String("topic")
	rawMessages := ctx.StringSlice("messages")
	statuses := ctx.IntSlice("statuses")

	if len(rawMessages) != len(statuses) {
		return fmt.Errorf("inequal number of messages to statuses (%#v : %#v)",
			rawMessages, statuses)
	}

	messages := []*src.Message{}
	for i := 0; i < len(rawMessages); i++ {
		messages = append(messages, &src.Message{
			Topic: topic,
			Payload: src.MessagePayload{
				Status: statuses[i],
				Body:   rawMessages[i],
			},
		})
	}

	pub, err := src.NewPublisher(address)
	if err != nil {
		return err
	}
	pub.PublishMessages(messages...)
	pub.Stop()

	return nil
}

func DoConsume(ctx *cli.Context) error {
	address := ctx.String("address")
	topic := ctx.String("topic")
	handlers := ctx.Uint("handlers")

	con, err := src.NewConsumer(topic, address, int(handlers))
	if err != nil {
		return err
	}
	defer con.Stop()
	<-con.StopChan()

	return nil
}
