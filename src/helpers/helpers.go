package helpers

import (
	"_/src/ts"
	"encoding/json"
	"regexp"
	"unicode"
	"unicode/utf8"
)

var keyMatchRegex = regexp.MustCompile(`"(\w+)":`)

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

func ConvertRecords(unconverted interface{}) ([]*ts.WriteRecord, bool) {
	ifaceArr, ok := unconverted.([]interface{})
	if !ok {
		return nil, false
	}
	converted := make([]*ts.WriteRecord, len(ifaceArr))
	for i, v := range ifaceArr {
		convertedMap, ok := v.(map[string]interface{})
		if !ok {
			return nil, false
		}
		dimensionsArr, isDimensionsOk := convertedMap["dimensions"].([]interface{})
		if !isDimensionsOk {
			return nil, false
		}
		dimensions := make([]ts.RecordDimension, len(dimensionsArr))
		for i, dimIface := range dimensionsArr {
			dimMap, ok := dimIface.(map[string]interface{})
			if !ok {
				return nil, false
			}
			dimensions[i] = ts.RecordDimension{}
			success := fromMapStr(dimMap, "name", &dimensions[i].Name) && fromMapStr(dimMap, "value", &dimensions[i].Value)
			if !success {
				return nil, false
			}
		}
		convertedValue := &ts.WriteRecord{
			Dimensions: dimensions,
		}
		success := fromMapInt64(convertedMap, "version", &convertedValue.Version)
		for name, remapTo := range map[string]*string{
			"measureName":  &convertedValue.MeasureName,
			"measureValue": &convertedValue.MeasureValue,
			"measureType":  &convertedValue.MeasureValueType,
			"time":         &convertedValue.Time,
			"timeUnit":     &convertedValue.TimeUnit,
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

func MarshalJsonChangeCase(value interface{}) ([]byte, error) {
	marshalled, err := json.Marshal(value)

	converted := keyMatchRegex.ReplaceAllFunc(
		marshalled,
		func(match []byte) []byte {
			// Empty keys are valid JSON, only lowercase if we do not have an
			// empty key.
			if len(match) > 2 {
				// Decode first rune after the double quotes
				r, width := utf8.DecodeRune(match[1:])
				r = unicode.ToLower(r)
				utf8.EncodeRune(match[1:width+1], r)
			}
			return match
		},
	)

	return converted, err
}
