package main

import (
	messagequeue "_/src/mq"
	timeseries "_/src/ts"
	"errors"
	"os"
	"strconv"
)

func fromMapStr(mapObj map[string]interface{}, prop string, dest *string) bool {
	val, ok := mapObj[prop].(string)
	if !ok {
		return false
	}
	*dest = val
	return true
}

func fromMapInt64(mapObj map[string]interface{}, prop string, dest *int64) bool {
	val, ok := mapObj[prop].(int64)
	if !ok {
		return false
	}
	*dest = val
	return true
}

func convertRecords(unconverted interface{}) ([]*timeseries.WriteRecord, bool) {
	ifaceArr, ok := unconverted.([]interface{})
	if !ok {
		return nil, false
	}
	converted := make([]*timeseries.WriteRecord, len(ifaceArr))
	for i, v := range ifaceArr {
		convertedMap, ok := v.(map[string]interface{})
		if !ok {
			return nil, false
		}
		dimensionsArr, isDimensionsOk := convertedMap["dimensions"].([]interface{})
		if !isDimensionsOk {
			return nil, false
		}
		dimensions := make([]timeseries.RecordDimension, len(dimensionsArr))
		for i, dimIface := range dimensionsArr {
			dimMap, ok := dimIface.(map[string]interface{})
			if !ok {
				return nil, false
			}
			dimensions[i] = timeseries.RecordDimension{}
			success := fromMapStr(dimMap, "name", &dimensions[i].Name) && fromMapStr(dimMap, "value", &dimensions[i].Value)
			if !success {
				return nil, false
			}
		}
		convertedValue := &timeseries.WriteRecord{
			Dimensions: dimensions,
		}
		success := fromMapInt64(convertedMap, "version", &convertedValue.Version)
		for name, remapTo := range map[string]*string{
			"measureName": &convertedValue.MeasureName,
			"measureValue": &convertedValue.MeasureValue,
			"measureType": &convertedValue.MeasureValueType,
			"time": &convertedValue.Time,
			"timeUnit": &convertedValue.TimeUnit,
		} {
			success = success && fromMapStr(convertedMap, name, remapTo)
		}
		if !success {
			return nil, false
		}
		converted[i] = convertedValue
	}
	return converted, true
}

func launchListener() {
	ts := timeseries.New()
	mq := messagequeue.New()
	mq.Listen(func(data interface{}) (interface{}, error) {
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
			records, recordsOk := convertRecords(dataTyped["records"])
			if !recordsOk {
				return nil, errors.New("Cannot get write records")
			}
			return nil, ts.Write(db, table, records)
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
