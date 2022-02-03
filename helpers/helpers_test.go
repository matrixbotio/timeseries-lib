package helpers

import (
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/matrixbotio/timeseries-lib/structs"
	"testing"
)

func TestConvertQueryOutput(t *testing.T) {
	// given
	nextToken := "mockedNextToken"
	queryId := "mockedQueryId"

	fieldName := "mockedFiledName"
	varcharType := timestreamquery.ScalarTypeVarchar
	var columnInfo []*timestreamquery.ColumnInfo
	columnInfo = append(columnInfo, &timestreamquery.ColumnInfo{
		Name: &fieldName,
		Type: &timestreamquery.Type{ScalarType: &varcharType},
	})

	value := "mockedValue"
	var data []*timestreamquery.Datum
	data = append(data, &timestreamquery.Datum{ScalarValue: &value})
	var rows []*timestreamquery.Row
	rows = append(rows, &timestreamquery.Row{Data: data})

	queryOutput := timestreamquery.QueryOutput{
		ColumnInfo: columnInfo,
		NextToken:  &nextToken,
		QueryId:    &queryId,
		Rows:       rows,
	}

	// when
	result := ConvertQueryOutput(&queryOutput)

	// then
	if result == nil {
		t.Errorf("Result should not be nil")
	}
}

func TestConvertWriteRecordsInput(t *testing.T) {
	// given
	db := "matrix"
	table := "candles"

	var records []*structs.WriteRecord

	var dimensions []structs.RecordDimension

	dimension := structs.RecordDimension{
		Name:  "exchange",
		Value: "binance-spot",
	}
	dimensions = append(dimensions, dimension)

	dimension = structs.RecordDimension{
		Name:  "symbol",
		Value: "LINKUSDT",
	}
	dimensions = append(dimensions, dimension)

	dimension = structs.RecordDimension{
		Name:  "interval",
		Value: "1m",
	}
	dimensions = append(dimensions, dimension)

	writeRecord := &structs.WriteRecord{
		Dimensions:       dimensions,
		MeasureName:      "low",
		MeasureValue:     "13.65",
		MeasureValueType: "DOUBLE",
		Time:             "1634065080000",
		TimeUnit:         "MILLISECONDS",
		Version:          0,
	}
	records = append(records, writeRecord)

	writeRecord = &structs.WriteRecord{
		Dimensions:       dimensions,
		MeasureName:      "high",
		MeasureValue:     "15.76",
		MeasureValueType: "DOUBLE",
		Time:             "1634065080000",
		TimeUnit:         "MILLISECONDS",
		Version:          0,
	}
	records = append(records, writeRecord)

	// when
	resultWriteRecords := ConvertWriteRecordsInput(db, table, records)

	// then
	t.Log("resultWriteRecords: ", resultWriteRecords)
	if resultWriteRecords == nil {
		t.Error("Result writeRecords is nil")
	}
	if len(resultWriteRecords.Records) != 2 {
		t.Error("Wrong records slice len: ", len(resultWriteRecords.Records))
	}
	if *resultWriteRecords.Records[0].Time != "1634065080000" {
		t.Error("Wrong time: ", *resultWriteRecords.Records[0].Time)
	}
	if len(resultWriteRecords.Records[0].Dimensions) != 3 {
		t.Error("Wrong dimensions slice len: ", len(resultWriteRecords.Records[0].Dimensions))
	}
	if *resultWriteRecords.Records[0].Dimensions[0].Name != "exchange" {
		t.Error("Wrong dimension name: ", *resultWriteRecords.Records[0].Dimensions[0].Name)
	}
	if *resultWriteRecords.Records[0].Dimensions[0].Value != "binance-spot" {
		t.Error("Wrong dimension value: ", *resultWriteRecords.Records[0].Dimensions[0].Value)
	}
	if *resultWriteRecords.Records[0].Dimensions[1].Name != "symbol" {
		t.Error("Wrong dimension name: ", *resultWriteRecords.Records[0].Dimensions[0].Name)
	}
	if *resultWriteRecords.Records[0].Dimensions[1].Value != "LINKUSDT" {
		t.Error("Wrong dimension value: ", *resultWriteRecords.Records[0].Dimensions[1].Value)
	}
}
