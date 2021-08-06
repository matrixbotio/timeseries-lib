package mq

import (
	"github.com/streadway/amqp"
	"os"
	"net/url"
	"crypto/tls"
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
	//
}

func New(){
	return &MQ{}
}
