package mq

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

func mqConnect() *amqp.Connection {
	var conn *amqp.Connection
	var err error
	var useTLS bool = true

	if os.Getenv("AMQP_TLS") == "0" {
		useTLS = false
	}

	dsn := "amqp"
	if useTLS {
		dsn += "s"
	}
	dsn += "://" + os.Getenv("AMQP_USER") + ":" +
		url.QueryEscape(os.Getenv("AMQP_PASS")) + "@" + os.Getenv("AMQP_HOST") + ":" +
		os.Getenv("AMQP_PORT") + "/"

	if useTLS {
		conn, err = amqp.DialTLS(dsn, &tls.Config{MinVersion: tls.VersionTLS12})
	} else {
		conn, err = amqp.Dial(dsn)
	}

	if err != nil {
		panic("Failed to connect to RabbitMQ: " + err.Error())
	}

	return conn
}

type MQ struct {
	conn *amqp.Connection
}

func messageProcess(msg amqp.Delivery, c *amqp.Channel, queueName string, cb func(interface{}) (interface{}, error)) {
	responseRoutingKey, rrkOk := msg.Headers["responseRoutingKey"].(string)
	needAnswer := msg.CorrelationId != "" && rrkOk && responseRoutingKey != ""
	var value interface{}
	err := json.Unmarshal(msg.Body, &value)
	if err != nil {
		if needAnswer {
			c.Publish(queueName + ".response", responseRoutingKey, false, true, amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body: []byte(err.Error()),
				Headers: amqp.Table{
					"code": -1,
					"name": "ERR_UNKNOWN",
				},
			})
		}
		return
	}
	res, cbErr := cb(value)
	if needAnswer {
		if cbErr != nil {
			c.Publish(queueName + ".response", responseRoutingKey, false, true, amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body: []byte(cbErr.Error()),
				Headers: amqp.Table{
					"code": -1,
					"name": "ERR_UNKNOWN",
				},
			})
			return
		}
		strRes, marshalErr := json.Marshal(res)
		if marshalErr != nil {
			c.Publish(queueName + ".response", responseRoutingKey, false, true, amqp.Publishing{
				CorrelationId: msg.CorrelationId,
				Body: []byte(marshalErr.Error()),
				Headers: amqp.Table{
					"code": -1,
					"name": "ERR_UNKNOWN",
				},
			})
			return
		}
		c.Publish(queueName + ".response", responseRoutingKey, false, true, amqp.Publishing{
			CorrelationId: msg.CorrelationId,
			Body: strRes,
			Headers: amqp.Table{
				"code": 0,
			},
		})
	}
}

func (mq *MQ) createChannel(i int, cb func(interface{}) (interface{}, error)) {
	c, err := mq.conn.Channel()
	for err != nil {
		fmt.Println("Warning: cannot create RMQ channel " + strconv.Itoa(i) + ". Retry in 3s")
		time.Sleep(time.Second * 3)
		c, err = mq.conn.Channel()
	}
	queue, declErr := c.QueueDeclare("timeseries", true, false, false, false, nil)
	if declErr != nil {
		fmt.Println("Warning: cannot declare the queue: " + declErr.Error() + ". Closing channel " + strconv.Itoa(i))
		c.Close()
		return
	}
	msgChan, consumeErr := c.Consume(queue.Name, "", true, false, true, false, nil)
	if consumeErr != nil {
		fmt.Println("Warning: cannot consume from queue: " + consumeErr.Error() + ". Closing channel " + strconv.Itoa(i))
		c.Close()
		return
	}
	for msg := range msgChan {
		go messageProcess(msg, c, queue.Name, cb)
	}
}

func (mq *MQ) Listen(cb func(interface{}) (interface{}, error)) {
	countStr := os.Getenv("AMQP_CHANNEL_COUNT")
	count := 10
	if countStr != "" {
		converted, err := strconv.Atoi(countStr)
		if err != nil {
			count = converted
		}
	}
	for i := 0; i < count; i++ {
		go mq.createChannel(i, cb)
	}
}

func New() *MQ {
	return &MQ{
		conn: mqConnect(),
	}
}
