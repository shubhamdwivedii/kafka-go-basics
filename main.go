package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "message-log"
	broker1Address = "localhost:9092"
)

// produce writes a message into the Kafka cluster every second, forever:
func produce(ctx context.Context) {
	// initialize a counter
	i := 0

	// initialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address},
		Topic:   topic,
	})

	for {
		// each kafka message has a key and value.
		// The key is used to decide which partition (and consequently, which broker) the message gets published on
		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(strconv.Itoa(i)),
			// create an arbitrary message payload for the value
			Value: []byte("{ message: \"" + strconv.Itoa(i) + "\""),
		})
		if err != nil {
			panic("could not write message " + err.Error())
		}

		// log a confirmation once the message is written
		fmt.Println("writes:", i)
		i++
		// sleep for a second
		time.Sleep(time.Second)
	}
}

// When creating a consumer, we need to specify it's group ID.

// This is because a single topic can have multiple consumers, and each consumers group ID ensures that
// multiple consumers belonging to the same groupID don't get repeated messages.

// consume consumes messages from the Kafka cluster, wheneer they're available:
func consume(ctx context.Context) {
	// initialized a new reader with the brokers and topic
	// the groupId identifies the consumer and prevents it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address},
		Topic:   topic,
		GroupID: "my-group",
	})

	for {
		// the `ReadMessage` method blocks until we receive the next even
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		// after receiving the message, log its value
		fmt.Println("received: ", string(msg.Value))
	}
}

type EventProcessor func(event map[string]interface{}) error

type Consumer struct {
	Reader       *kafka.Reader
	ProcessEvent EventProcessor
}

func (c *Consumer) Init(config kafka.ReaderConfig) {
	c.Reader = kafka.NewReader(config)
}

func (c *Consumer) Consume() {
	for {
		m, err := c.Reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err, "Error while reading message")
			break
		}

		event, err := convertToMap(m.Value)
		if err != nil {
			log.Println(err, "Error while unmarshalling event")
			continue
		}

		retries := 3
		for retries > 0 {
			err = c.ProcessEvent(event)
			if err != nil && strings.Contains(err.Error(), "i/o timeout") {
				// i/o timeout error >> retry
				log.Println(err, "Processing of event failed")
				if retries >= 1 {
					log.Println("retrying...")
				}
				retries--
			} else {
				// successfull or other error >> continue with next message
				log.Println("continuing...")
				break
			}
		}
	}
}

func convertToMap(bytes []byte) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	err := json.Unmarshal(bytes, &m)
	return m, err
}

func main() {
	// create a new context
	ctx := context.Background()
	consumer := Consumer{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker1Address},
			Topic:   topic,
			GroupID: "my-group",
		}),
		ProcessEvent: func(event map[string]interface{}) error {
			fmt.Println("Event::::", event)
			return nil
		},
	}
	// produce messages in new go routine, since both the produce and consume functions are blocking
	go produce(ctx)
	consumer.Consume()
	// consume(ctx)
}