package rabbitmqutils

import (
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error) {
	if err != nil {
		log.Fatalf("%s", err)
	}
}

type rabbitMQ struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Query   map[string]amqp.Queue
}

func (i *rabbitMQ) Close() {
	failOnError(i.Channel.Close())
	failOnError(i.Conn.Close())
}

func declareQueue(ch *amqp.Channel, key string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		key,
		false,
		false,
		false,
		false,
		nil,
	)
}

func (i *rabbitMQ) NewQueue(queue string) {
	q, err := declareQueue(i.Channel, queue)
	failOnError(err)
	if _, exists := i.Query[queue]; exists == false {
		i.Query[queue] = q
	}
}

func (i *rabbitMQ) Publish(queue string, body interface{}, contenttype string) {
	var byteBody []byte

	switch body.(type) {
	case string:
		byteBody = []byte(body.(string))

	}
	err := i.Channel.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ContentType: contenttype,
			Body:        byteBody,
		})

	failOnError(err)
}

func (i *rabbitMQ) Receive(queue string) (<-chan amqp.Delivery, error) {
	q, err := declareQueue(i.Channel, queue)
	if err != nil {
		return nil, err
	}

	if _, exists := i.Query[queue]; exists == false {
		i.Query[queue] = q
	}

	return i.Channel.Consume(
		queue, // queue
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

func NewRabbitMQ(connect string) rabbitMQ {
	conn, err := amqp.Dial(connect)
	failOnError(err)

	channel, err := conn.Channel()
	failOnError(err)

	return rabbitMQ{Conn: conn, Channel: channel, Query: make(map[string]amqp.Queue)}
}
