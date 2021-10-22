package mq

import (
	"_/src/helpers"
	"_/src/logger"
	"github.com/Sagleft/rmqworker-lib"
	"github.com/matrixbotio/constants-lib"
	"net/url"
	"os"
	"strconv"
)

type RMQ struct {
	Handler *rmqworker.RMQHandler
	Workers []*rmqworker.RMQWorker

	queueName string
}

var log = logger.Logger

const defaultQueueName = "timestream"
const defaultChannelCount = 10

func New(messageHandler func(workerDeliveryHandler rmqworker.RMQDeliveryHandler) (interface{}, error)) (*RMQ, rmqworker.APIError) {
	rmqHandler, err := connect()
	if err != nil {
		return nil, err
	}
	rmq := RMQ{Handler: rmqHandler}
	err = rmq.initQueues()
	callback := func(worker *rmqworker.RMQWorker, workerDeliveryHandler rmqworker.RMQDeliveryHandler) {
		response, err := messageHandler(workerDeliveryHandler)
		var apiError helpers.ApiError
		if err != nil {
			apiError = constants.Error("BASE_INTERNAL_ERROR", err.Error())
		}
		responseRoutingKey, getRrkErr := workerDeliveryHandler.GetResponseRoutingKeyHeader()
		if getRrkErr != nil {
			log.Verbose(getRrkErr)
			return
		}
		task := rmqworker.RMQPublishResponseTask{
			ExchangeName:       defaultQueueName + ".response",
			ResponseRoutingKey: responseRoutingKey,
			CorrelationID:      workerDeliveryHandler.GetCorrelationID(),
			MessageBody:        response,
		}
		rmqHandler.SendRMQResponse(&task, apiError)
	}
	rmq.initWorkers(callback)
	return &rmq, nil
}

func connect() (*rmqworker.RMQHandler, rmqworker.APIError) {
	var err rmqworker.APIError
	connData := rmqworker.RMQConnectionData{
		User:     os.Getenv("AMQP_USER"),
		Password: url.QueryEscape(os.Getenv("AMQP_PASS")),
		Host:     os.Getenv("AMQP_HOST"),
		Port:     os.Getenv("AMQP_PORT"),
		UseTLS:   os.Getenv("AMQP_TLS"),
	}
	rmqHandler, err := rmqworker.NewRMQHandler(connData, log)
	return rmqHandler, err
}

func (rmq *RMQ) initQueues() rmqworker.APIError {
	rmq.queueName = os.Getenv("AMPQ_QUEUE_NAME")
	if rmq.queueName == "" {
		rmq.queueName = defaultQueueName
	}
	queues := []string{
		rmq.queueName,
	}
	err := rmq.Handler.DeclareQueues(queues)
	return err
}

func (rmq *RMQ) initWorkers(cb rmqworker.RMQDeliveryCallback) helpers.ApiError {
	channelCountStr := os.Getenv("AMQP_CHANNEL_COUNT")
	channelCount := defaultChannelCount

	var err error
	if channelCountStr != "" {
		channelCount, err = strconv.Atoi(channelCountStr)
		if err != nil {
			log.Warn("Error getting channel count from value: " + channelCountStr +
				" using default value: " + strconv.Itoa(defaultChannelCount))
			channelCount = defaultChannelCount
		}
	}
	for i := 0; i < channelCount; i++ {
		rmqHandler, err := rmq.Handler.NewRMQHandler()
		if err != nil {
			return helpers.ApiError(err)
		}
		rmqWorker, err := rmqHandler.NewRMQWorker(rmq.queueName, cb)
		if err != nil {
			return helpers.ApiError(err)
		}
		rmqWorker.Subscribe()
		go rmqWorker.Listen()
		rmq.Workers = append(rmq.Workers, rmqWorker)
	}
	return nil
}
