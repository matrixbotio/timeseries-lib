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
	var useTLS = true
	var skipTlsVerify = false

	if os.Getenv("AMQP_TLS") == "0" {
		useTLS = false
	}

	if os.Getenv("AMQP_SKIP_TLS_VERIFY") == "true" {
		skipTlsVerify = true
	}

	dsn := "amqp"
	if useTLS {
		dsn += "s"
	}
	dsn += "://" + os.Getenv("AMQP_USER") + ":" +
		url.QueryEscape(os.Getenv("AMQP_PASS")) + "@" + os.Getenv("AMQP_HOST") + ":" +
		os.Getenv("AMQP_PORT") + "/"

	if useTLS {
		conn, err = amqp.DialTLS(dsn, &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: skipTlsVerify})
	} else {
		conn, err = amqp.Dial(dsn)
	}

	if err != nil {
		panic("Failed to connect to RabbitMQ: " + err.Error())
	}

	return conn
}

func publish(c *amqp.Channel, queueName, responseRoutingKey, cid string, body []byte, headers headers) {
	log.Verbose("publishing data to queue " + queueName + ".response" + " and responseRoutingKey " + responseRoutingKey + ":\n" + string(body))
	err := c.Publish(queueName+".response", responseRoutingKey, false, false, amqp.Publishing{
		CorrelationId: cid,
		Body:          body,
		Headers:       headers,
	})
	if err != nil {
		strBody, _ := json.Marshal(string(body))
		log.Warn("Cannot publish the message " + string(strBody) + " to AMQP: " + err.Error())
	}
}

func publishErr(c *amqp.Channel, qn, rrk, cid string, err *constants.APIError) {
	log.Warn(err)
	headers := headers{
		"code": err.Code,
		"name": err.Name,
	}
	publish(c, qn, rrk, cid, []byte(err.Message), headers)
}

func messageProcess(msg amqp.Delivery, channel *amqp.Channel, queueName string, callBack func(interface{}) (interface{}, error)) {
	responseRoutingKey, rrkOk := msg.Headers["responseRoutingKey"].(string)
	needAnswer := msg.CorrelationId != "" && rrkOk && responseRoutingKey != ""
	var value interface{}
	err := json.Unmarshal(msg.Body, &value)
	if err != nil {
		if needAnswer {
			publishErr(channel, queueName, responseRoutingKey, msg.CorrelationId, constants.Error("DATA_PARSE_ERR"))
		} else {
			log.Warn("Failed to unmarshal JSON and answer isn't needed. Body is:\n" + string(msg.Body))
		}
		return
	}
	res, cbErr := callBack(value)
	if needAnswer {
		if cbErr != nil {
			publishErr(channel, queueName, responseRoutingKey, msg.CorrelationId, constants.Error("DATA_HANDLE_ERR", cbErr.Error()))
			return
		}
		if res == nil {
			res = "OK"
		}
		bytesRes, marshalErr := json.Marshal(res)
		if marshalErr != nil {
			publishErr(channel, queueName, responseRoutingKey, msg.CorrelationId, constants.Error("DATA_ENCODE_ERR"))
			return
		}
		publish(channel, queueName, responseRoutingKey, msg.CorrelationId, bytesRes, headers{
			"code": 0,
		})
	} else {
		log.Verbose(
			"Answer not needed. Data: {\"msg.CorrelationId\":\"" +
				msg.CorrelationId +
				"\",\"responseRoutingKey\":\"" +
				responseRoutingKey +
				"\",\"rrkOk\":" +
				strconv.FormatBool(rrkOk) +
				"}",
		)
	}
}

func (mq *MQ) createChannel(i int, cb func(interface{}) (interface{}, error)) {
	c, err := mq.conn.Channel()
	for err != nil {
		log.Warn("Cannot create RMQ channel " + strconv.Itoa(i) + ". Retry in 3s")
		time.Sleep(time.Second * 3)
		c, err = mq.conn.Channel()
	}
	queue, declErr := c.QueueDeclare("timeseries.test", true, false, false, false, nil)
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
