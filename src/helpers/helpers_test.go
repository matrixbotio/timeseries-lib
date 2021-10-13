package helpers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

func TestConvertRow(t *testing.T) {
	// given
	rowStr := "{\n" +
		"    \"dimensions\": [\n" +
		"        {\n" +
		"          \"name\": \"exchange\",\n" +
		"          \"value\": \"binance-spot\"\n" +
		"        },\n" +
		"        {\n" +
		"          \"name\": \"symbol\",\n" +
		"          \"value\": \"LINKUSDT\"\n" +
		"        },\n" +
		"        {\n" +
		"          \"name\": \"interval\",\n" +
		"          \"value\": \"1m\"\n" +
		"        }\n" +
		"      ],\n" +
		"      \"measureName\": \"open\",\n" +
		"      \"measureType\": \"DOUBLE\",\n" +
		"      \"measureValue\": \"24.54000000\",\n" +
		"      \"time\": \"1634065080000\",\n" +
		"      \"timeUnit\": \"MILLISECONDS\",\n" +
		"      \"version\": 6\n" +
		"    }"
	var row map[string]interface{}
	err := json.Unmarshal([]byte(rowStr), &row)
	if err != nil {
		t.Errorf(err.Error())
	}

	// when
	writeRecord, err := convertRow(row)

	// then
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	if writeRecord.Time != "1634065080000" {
		t.Errorf("Time converted incorrectly: " + writeRecord.Time)
		return
	}
	if writeRecord.Version != 6 {
		t.Errorf("Version converted incorrectly: " + strconv.Itoa(int(writeRecord.Version)))
		return
	}
	if writeRecord.MeasureValue != "24.54000000" {
		t.Errorf("MeasureValue converted incorrectly: " + writeRecord.MeasureValue)
		return
	}
	if len(writeRecord.Dimensions) != 3 || writeRecord.Dimensions[0].Name != "exchange" ||
		writeRecord.Dimensions[0].Value != "binance-spot" {
		t.Errorf("Dimenstions parsed incorrectly: " + fmt.Sprintf("%#v", writeRecord.Dimensions))
		return
	}
	if writeRecord.Dimensions[0].Name != "exchange" {
		t.Errorf("")
		return
	}
}
