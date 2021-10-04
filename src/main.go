package main

import (
	"_/src/helpers"
	"_/src/logger"
	messagequeue "_/src/mq"
	timeseries "_/src/ts"
	"encoding/json"
	"errors"
	"os"
	"strconv"
)

var log = logger.Logger

func launchListener() {
	ts := timeseries.New()
	mq := messagequeue.New()
	mq.Listen(func(data interface{}) (interface{}, error) {
		jsondata, err := json.Marshal(data)
		if err != nil {
			jsondata = []byte("<cannot marshal data to JSON>")
		}
		log.Verbose("Got new data: " + string(jsondata))
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
			db, dbOk := dataTyped["db"].(string)
			if !dbOk {
				return nil, errors.New("Cannot get write db")
			}
			table, tableOk := dataTyped["table"].(string)
			if !tableOk {
				return nil, errors.New("Cannot get write table")
			}
			records, recordsOk := helpers.ConvertRecords(dataTyped["records"])
			if !recordsOk {
				return nil, errors.New("Cannot get write records")
			}
			return nil, ts.Write(db, table, records)
		} else if reqType == "describe_db" {
			db, dbOk := dataTyped["db"].(string)
			if !dbOk {
				return nil, errors.New("Cannot get the db to describe")
			}
			return nil, ts.DescribeTSDB(db)
		} else if reqType == "describe_table" {
			db, dbOk := dataTyped["db"].(string)
			if !dbOk {
				return nil, errors.New("Cannot get the db to describe")
			}
			table, tableOk := dataTyped["table"].(string)
			if !tableOk {
				return nil, errors.New("Cannot get the table to describe")
			}
			return nil, ts.DescribeTSTable(db, table)
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
	log.Log("Started successfully with " + strconv.Itoa(count) + " listeners")
	forever := make(chan bool)
	// Background work
	<-forever
}
