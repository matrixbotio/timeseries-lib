package mq

import (
	"crypto/tls"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"time"

	"_/src/logger"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

var log = logger.Logger

type MQ struct {
	conn *amqp.Connection
}

type headers = amqp.Table

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

func publish(c *amqp.Channel, qn, rrk, cid string, body []byte, headers headers){
	err := c.Publish(qn + ".response", rrk, false, true, amqp.Publishing{
		CorrelationId: cid,
		Body: body,
		Headers: headers,
	})
	if err != nil {
		strBody, _ := json.Marshal(string(body))
		log.Warn("Cannot publish the message " + string(strBody) + " to AMQP: " + err.Error())
	}
}

func publishErr(c *amqp.Channel, qn, rrk, cid string, err *constants.APIError){
	log.Warn(err)
	headers := headers {
		"code": err.Code,
		"name": err.Name,
	}
	publish(c, qn, rrk, cid, []byte(err.Message), headers)
}

func messageProcess(msg amqp.Delivery, c *amqp.Channel, queueName string, cb func(interface{}) (interface{}, error)) {
	responseRoutingKey, rrkOk := msg.Headers["responseRoutingKey"].(string)
	needAnswer := msg.CorrelationId != "" && rrkOk && responseRoutingKey != ""
	var value interface{}
	err := json.Unmarshal(msg.Body, &value)
	if err != nil {
		if needAnswer {
			publishErr(c, queueName, responseRoutingKey, msg.CorrelationId, constants.Error("DATA_PARSE_ERR"))
		}
		return
	}
	res, cbErr := cb(value)
	if needAnswer {
		if cbErr != nil {
			publishErr(c, queueName, responseRoutingKey, msg.CorrelationId, constants.Error("DATA_HANDLE_ERR", cbErr.Error()))
			return
		}
		strRes, marshalErr := json.Marshal(res)
		if marshalErr != nil {
			publishErr(c, queueName, responseRoutingKey, msg.CorrelationId, constants.Error("DATA_ENCODE_ERR"))
			return
		}
		publish(c, queueName, responseRoutingKey, msg.CorrelationId, strRes, headers{
			"code": 0,
		})
	}
}

func (mq *MQ) createChannel(i int, cb func(interface{}) (interface{}, error)) {
	c, err := mq.conn.Channel()
	for err != nil {
		log.Warn("Cannot create RMQ channel " + strconv.Itoa(i) + ". Retry in 3s")
		time.Sleep(time.Second * 3)
		c, err = mq.conn.Channel()
	}
	queue, declErr := c.QueueDeclare("timeseries", true, false, false, false, nil)
	if declErr != nil {
		log.Warn("Cannot declare the queue: " + declErr.Error() + ". Closing channel " + strconv.Itoa(i))
		c.Close()
		return
	}
	msgChan, consumeErr := c.Consume(queue.Name, "", true, false, true, false, nil)
	if consumeErr != nil {
		log.Warn("Cannot consume from queue: " + consumeErr.Error() + ". Closing channel " + strconv.Itoa(i))
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
