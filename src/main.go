package main

import (
	messagequeue "_/src/mq"
	timeseries "_/src/ts"
	"errors"
	"os"
	"strconv"
)

func launchListener() {
	ts := timeseries.New()
	mq := messagequeue.New()
	mq.Listen(func(data interface {}) (interface{}, error) {
		dataTyped, ok := data.(map[string]interface{})
		if !ok {
			return nil, errors.New("Cannot convert incoming data to map")
		}
		reqType, typeOk := dataTyped["type"].(string)
		if !typeOk {
			return nil, errors.New("Cannot get request type")
		}
		if reqType == "query" {
			query, queryOk := dataTyped["data"].(string)
			if !queryOk {
				return nil, errors.New("Cannot get request query")
			}
			return ts.Query(query)
		} else if reqType == "write" {
		} else {
			return nil, errors.New("Unknown request type " + reqType)
		}
	})
}

func main() {
	countStr := os.Getenv("LISTENER_COUNT")
	count := 10
	if countStr != "" {
		converted, err := strconv.Atoi(countStr)
		if err != nil {
			count = converted
		}
	}
	for i := 0; i < count; i++ {
		go launchListener()
	}
	forever := make(chan bool)
	// Background work
	<-forever
}
