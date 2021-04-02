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
	Query   []amqp.Queue
}

func (i *rabbitMQ) Close() {
	failOnError(i.Channel.Close())
	failOnError(i.Conn.Close())
}

func (i *rabbitMQ) NewQueue(key string) {
	q, err := i.Channel.QueueDeclare(
		key,
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err)
	i.Query = append(i.Query, q)
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

func NewRabbitMQ(connect string) rabbitMQ {
	conn, err := amqp.Dial(connect)
	failOnError(err)

	channel, err := conn.Channel()
	failOnError(err)

	return rabbitMQ{Conn: conn, Channel: channel}
}

//var RabbitMQ rabbitMQ
