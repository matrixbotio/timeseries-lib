package helpers

import (
	"_/structs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"github.com/matrixbotio/constants-lib"
)

type ApiError *constants.APIError

// ConvertQueryOutput - convert query output to MatrixBot structs
func ConvertQueryOutput(queryOutput *timestreamquery.QueryOutput) *structs.QueryOutput {
	var columnInfo []*structs.ColumnInfo
	for i := range queryOutput.ColumnInfo {
		tsColumnInfo := queryOutput.ColumnInfo[i]
		columnInfo = append(columnInfo, &structs.ColumnInfo{
			Name: tsColumnInfo.Name,
			Type: tsColumnInfo.Type.ScalarType,
		})
	}
	var rows []*structs.Row
	for i := range queryOutput.Rows {
		tsRow := queryOutput.Rows[i]
		var data []*string
		for j := range tsRow.Data {
			tsRowData := tsRow.Data[j]
			data = append(data, tsRowData.ScalarValue)
		}
		rows = append(rows, &structs.Row{Data: data})
	}
	return &structs.QueryOutput{
		ColumnInfo: columnInfo,
		NextToken:  queryOutput.NextToken,
		Rows:       rows,
	}
}

// ConvertWriteRecordsInput - convert write records to TimeStream structs
func ConvertWriteRecordsInput(db string, table string, records []*structs.WriteRecord) *timestreamwrite.WriteRecordsInput {
	recordsSlice := make([]*timestreamwrite.Record, 0)
	for _, writeRecord := range records {
		// create record
		record := &timestreamwrite.Record{
			Dimensions:       make([]*timestreamwrite.Dimension, 0),
			MeasureName:      aws.String(writeRecord.MeasureName),
			MeasureValue:     aws.String(writeRecord.MeasureValue),
			MeasureValueType: aws.String(writeRecord.MeasureValueType),
			Time:             aws.String(writeRecord.Time),
			TimeUnit:         aws.String(writeRecord.TimeUnit),
			Version:          aws.Int64(int64(writeRecord.Version)),
		}
		// add dimensions
		dimensionValueType := "VARCHAR"
		for _, dimension := range writeRecord.Dimensions {
			name := dimension.Name
			value := dimension.Value
			record.Dimensions = append(record.Dimensions, &timestreamwrite.Dimension{
				Name:               &name,
				Value:              &value,
				DimensionValueType: &dimensionValueType,
			})
		}
		// append values to recordsSlice
		recordsSlice = append(recordsSlice, record)
	}

	return &timestreamwrite.WriteRecordsInput{
		DatabaseName: &db,
		TableName:    &table,
		Records:      recordsSlice,
	}
}
