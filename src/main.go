package main

import (
	"_/src/helpers"
	"_/src/logger"
	messagequeue "_/src/mq"
	timeseries "_/src/ts"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/matrixbotio/rmqworker-lib"
	"strconv"
	"time"
)

var log = logger.Logger

func main() {
	ts := timeseries.New()
	rmq, err := messagequeue.New(func(handler rmqworker.RMQDeliveryHandler) (interface{}, error) {
		return handleMessage(handler, ts)
	})
	if err != nil {
		log.Error(err)
		time.Sleep(300 * time.Millisecond) // wait while logged
		panic(fmt.Sprintf("%#v", err))
	}
	log.Log("Started successfully with " + strconv.Itoa(len(rmq.Workers)) + " workers.")
	forever := make(chan bool)
	// Background work
	<-forever
}

func handleMessage(handler rmqworker.RMQDeliveryHandler, ts *timeseries.TS) (interface{}, error) {
	log.Verbose("Received new request")
	var data interface{}
	err := json.Unmarshal(handler.GetMessageBody(), &data)
	if err != nil {
		return nil, errors.New("Cannot unmarshal data from JSON: " + err.Error())
	}
	dataTyped, ok := data.(map[string]interface{})
	if !ok {
		return nil, errors.New("Cannot convert incoming data to map")
	}
	reqType, typeOk := dataTyped["type"].(string)
	if !typeOk {
		return nil, errors.New("Cannot get request type")
	}
	log.Verbose("Got request of type " + reqType)
	if reqType == "query" {
		query, queryOk := dataTyped["data"].(string)
		if !queryOk {
			return nil, errors.New("Cannot get request query")
		}
		var nextToken *string
		if dataTyped["nextToken"] != nil {
			nt := dataTyped["nextToken"].(string)
			nextToken = &nt
		} else {
			nextToken = nil
		}
		log.Verbose("Start query request")
		start := time.Now()
		r, err := ts.Query(query, nextToken)
		log.Verbose(fmt.Sprintf("Finish query request in %s", time.Since(start)))
		return r, err
	} else if reqType == "write" {
		db, dbOk := dataTyped["db"].(string)
		if !dbOk {
			return nil, errors.New("Cannot get write db")
		}
		table, tableOk := dataTyped["table"].(string)
		if !tableOk {
			return nil, errors.New("Cannot get write table")
		}
		records, err := helpers.ConvertRecords(dataTyped["records"])
		if err != nil {
			return nil, errors.New("Cannot get write records: " + err.Error())
		}
		log.Verbose("Start write request")
		start := time.Now()
		err = ts.Write(db, table, records)
		log.Verbose(fmt.Sprintf("Finish write request in %s", time.Since(start)))
		return nil, err
	} else if reqType == "describe_db" {
		db, dbOk := dataTyped["db"].(string)
		if !dbOk {
			return nil, errors.New("Cannot get the db to describe")
		}
		log.Verbose("Start describe_db request")
		start := time.Now()
		err := ts.DescribeTSDB(db)
		log.Verbose(fmt.Sprintf("Finish describe_db request in %s", time.Since(start)))
		return nil, err
	} else if reqType == "describe_table" {
		db, dbOk := dataTyped["db"].(string)
		if !dbOk {
			return nil, errors.New("Cannot get the db to describe")
		}
		table, tableOk := dataTyped["table"].(string)
		if !tableOk {
			return nil, errors.New("Cannot get the table to describe")
		}
		log.Verbose("Start describe_table request")
		start := time.Now()
		err := ts.DescribeTSTable(db, table)
		log.Verbose(fmt.Sprintf("Finish describe_table request in %s", time.Since(start)))
		return nil, err
	} else {
		return nil, errors.New("Unknown request type " + reqType)
	}
}
