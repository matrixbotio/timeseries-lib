package ts

import (
	"_/src/logger"
	"_/src/structs"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"golang.org/x/net/http2"
)

var log = logger.Logger

// TS - TS handler struct
type TS struct {
	q *timestreamquery.TimestreamQuery
	w *timestreamwrite.TimestreamWrite
}

func createTSSession() (*timestreamquery.TimestreamQuery, *timestreamwrite.TimestreamWrite) {
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	err := http2.ConfigureTransport(tr)
	if err != nil {
		panic("Failed to configure http2 transport: " + err.Error())
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(os.Getenv("AWS_REGION")),
		Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
		MaxRetries:  aws.Int(10),
		HTTPClient:  &http.Client{Transport: tr},
	})

	if err != nil {
		panic("Failed to create AWS session: " + err.Error())
	}

	return timestreamquery.New(sess), timestreamwrite.New(sess)
}

// New - create new TS handler
func New() *TS {
	q, w := createTSSession()
	return &TS{
		q: q,
		w: w,
	}
}

// Query - select ts data
func (ts *TS) Query(query string, nextToken *string) (*structs.QueryOutput, error) {
	tsResult, err := ts.q.Query(&timestreamquery.QueryInput{
		QueryString: &query,
		NextToken:   nextToken,
	})
	if err != nil {
		return nil, errors.New("failed to exec ts query: " + err.Error())
	}
	return convertQueryOutput(tsResult), nil
}

// Write ts records
func (ts *TS) Write(db, table string, records []*structs.WriteRecord) error {
	writeRecordsInput := convertWriteRecordsInput(db, table, records)
	_, err := ts.w.WriteRecords(writeRecordsInput)
	if err != nil {
		log.Verbose("Failed to write records: " + err.Error() + "\n" + fmt.Sprintf("%#v", writeRecordsInput))
		return errors.New("failed to write ts records: " + err.Error())
	}
	return nil
}

// DescribeTSTable - describe timeseries db table
func (ts *TS) DescribeTSTable(dbName, tableName string) error {
	_, err := ts.w.DescribeTable(&timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tableName),
	})
	if err == nil {
		return nil
	}
	// check error
	_, isTableNotExists := err.(*timestreamwrite.ResourceNotFoundException)
	if !isTableNotExists {
		return errors.New("failed to describe tsdb table: " + err.Error())
	}
	// Create table if table doesn't exist
	_, err = ts.w.CreateTable(&timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tableName),
	})
	if err != nil {
		return errors.New("Error while creating table:" + err.Error())
	}
	return nil
}

// DescribeTSDB - describe timeseries db
func (ts *TS) DescribeTSDB(dbName string) error {
	_, err := ts.w.DescribeDatabase(&timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	if err == nil {
		return nil
	}
	// check error
	_, isDBNotExists := err.(*timestreamwrite.ResourceNotFoundException)
	if !isDBNotExists {
		return errors.New("failed to describe tsdb: " + err.Error())
	}
	// Create database if database doesn't exist
	_, err = ts.w.CreateDatabase(&timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	if err != nil {
		return errors.New("failed to create tsdb: " + err.Error())
	}
	return nil
}

func convertQueryOutput(queryOutput *timestreamquery.QueryOutput) *structs.QueryOutput {
	var columnInfos []structs.ColumnInfo
	for i := range queryOutput.ColumnInfo {
		tsColumnInfo := queryOutput.ColumnInfo[i]
		columnInfo := structs.ColumnInfo{}
		if tsColumnInfo.Name != nil {
			columnInfo.Name = *tsColumnInfo.Name
		}
		if tsColumnInfo.Type.ScalarType != nil {
			columnInfo.Type = *tsColumnInfo.Type.ScalarType
		}
	}
	var rows []structs.Row
	for i := range queryOutput.Rows {
		tsRow := queryOutput.Rows[i]
		var data []string
		for j := range tsRow.Data {
			tsRowData := tsRow.Data[j]
			if tsRowData.ScalarValue != nil {
				data = append(data, *tsRowData.ScalarValue)
			}
		}
		rows = append(rows, structs.Row{Data: data})
	}
	result := &structs.QueryOutput{
		ColumnInfo: columnInfos,
		Rows:       rows,
	}
	if queryOutput.NextToken != nil {
		result.NextToken = *queryOutput.NextToken
	}
	return result
}

func convertWriteRecordsInput(db string, table string, records []*structs.WriteRecord) *timestreamwrite.WriteRecordsInput {
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
