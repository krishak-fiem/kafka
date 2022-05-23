package kafka

import (
	"context"
	"fmt"
	constants "github.com/krishak-fiem/constants/go"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func init() {
	conn, err := kafka.Dial("tcp", "localhost:29092")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{{Topic: string(constants.USER_CREATED), NumPartitions: 1, ReplicationFactor: 1}}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}

func CreateReader(brokers []string, topic string) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return r
}

func MessageReader(brokers []string, topic string, handler func(message kafka.Message) error) {
	r := CreateReader(brokers, topic)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		err = handler(m)
		if err != nil {
			fmt.Println("handler", err.Error())
		}
	}
	defer r.Close()
}

func CreateWriter(broker string, topic constants.KafkaTopics) *kafka.Writer {
	w := &kafka.Writer{
		Addr:  kafka.TCP(broker),
		Topic: string(topic),
	}

	return w
}

func MessageWriter(broker string, topic constants.KafkaTopics, key constants.KafkaKeys, message []byte) {
	w := CreateWriter(broker, topic)

	fmt.Println("message writer", w.Topic)
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: message,
		},
	)
	if err != nil {
		fmt.Println("failed to write messages:", err)
	}
}
