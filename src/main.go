package main

import (
	"os"
	"strconv"
	timeseries "_/src/ts"
	messagequeue "_/src/mq"
)

func launchListener() {
	ts := timeseries.New()
	mq := messagequeue.New()
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
