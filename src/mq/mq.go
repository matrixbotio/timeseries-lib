package mq

import (
	"crypto/tls"
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

func (mq *MQ) createChannel(i int) {
	c, err := mq.conn.Channel()
	for err != nil {
		fmt.Println("Warning: cannot create RMQ channel " + strconv.Itoa(i) + ". Retry in 3s")
		time.Sleep(time.Second * 3)
		c, err = mq.conn.Channel()
	}
	queue, declErr := c.QueueDeclare("timeseries", true, false, false, false, nil)
	if declErr != nil {
		fmt.Println("Warning: cannot declare timeseries queue: " + declErr.Error() + ". Closing channel " + strconv.Itoa(i))
		c.Close()
		return
	}
	queue
}

func (mq *MQ) Listen(cb func(interface{}) interface{}) {
	countStr := os.Getenv("MQ_CHANNEL_COUNT")
	count := 10
	if countStr != "" {
		converted, err := strconv.Atoi(countStr)
		if err != nil {
			count = converted
		}
	}
	for i := 0; i < count; i++ {
		go mq.createChannel(i)
	}
}

func New() *MQ {
	conn := mqConnect()
	return &MQ{
		conn: conn,
	}
}
