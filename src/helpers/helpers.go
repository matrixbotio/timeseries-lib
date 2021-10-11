package helpers

import (
	"_/src/structs"
	"errors"
	"strconv"
)

func fromMapInt64(mapObj map[string]interface{}, prop string, dest *int64) bool {
	val, ok := mapObj[prop].(int64)
	if !ok {
		return false
	}
	*dest = val
	return true
}

func ConvertRecords(unconverted interface{}) ([]*structs.WriteRecord, error) {
	ifaceArr, ok := unconverted.([]interface{})
	if !ok {
		return nil, errors.New("Write records should be an array")
	}
	converted := make([]*structs.WriteRecord, len(ifaceArr))
	for i, row := range ifaceArr {
		convertedRow, err := convertRow(row)
		if err != nil {
			return nil, errors.New("Cannot convert write record row on index " + strconv.Itoa(i) + ": " + err.Error())
		}
		converted[i] = convertedRow
	}
	return converted, nil
}

func convertRow(row interface{}) (*structs.WriteRecord, error) {
	convertedMap, ok := row.(map[string]interface{})
	if !ok {
		return nil, errors.New("row should be an object")
	}
	dimensions, isDimensionsOk := convertedMap["dimensions"].([]interface{})
	if !isDimensionsOk {
		return nil, errors.New("dimensions should be set")
	}
	convertedDimensions := make([]structs.RecordDimension, len(dimensions))
	for i, dimension := range dimensions {
		convertedDimension, err := convertDimension(dimension)
		if err != nil {
			return nil, errors.New("Error converting dimension at index " + strconv.Itoa(i) + ": " + err.Error())
		}
		convertedDimensions[i] = *convertedDimension
	}
	convertedValue := &structs.WriteRecord{
		Dimensions: convertedDimensions,
	}
	success := fromMapInt64(convertedMap, "version", &convertedValue.Version)
	if !success {
		return nil, errors.New("Cannot get version")
	}
	for name, remapTo := range map[string]string{
		"measureName":  convertedValue.MeasureName,
		"measureValue": convertedValue.MeasureValue,
		"measureType":  convertedValue.MeasureValueType,
		"time":         convertedValue.Time,
		"timeUnit":     convertedValue.TimeUnit,
	} {
		remapTo, success = convertedMap[name].(string)
		if !success {
			return nil, errors.New("Cannot convert " + name + " with value " + remapTo)
		}
	}
	return convertedValue, nil
}

func convertDimension(dimension interface{}) (*structs.RecordDimension, error) {
	dimMap, ok := dimension.(map[string]interface{})
	if !ok {
		return nil, errors.New("dimension should be an object")
	}
	convertedDimension := structs.RecordDimension{}
	var success bool
	convertedDimension.Name, success = dimMap["name"].(string)
	if !success {
		return nil, errors.New("Cannot get dimension name")
	}
	convertedDimension.Value, success = dimMap["value"].(string)
	if !success {
		return nil, errors.New("Cannot get dimension value")
	}
	return &convertedDimension, nil
}
