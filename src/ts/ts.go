package ts

import (
	"errors"
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
func (ts *TS) Query(query string, nextToken *string) (*QueryOutput, error) {
	tsResult, err := ts.q.Query(&timestreamquery.QueryInput{
		QueryString: &query,
		NextToken:   nextToken,
	})
	if err != nil {
		return nil, errors.New("failed to exec ts query: " + err.Error())
	}
	return ConvertQueryOutput(tsResult), nil
}

// Write ts records
func (ts *TS) Write(db, table string, records []*WriteRecord) error {
	recordsSlice := make([]*timestreamwrite.Record, 0)
	for _, writeRecord := range records {
		// create record
		record := &timestreamwrite.Record{
			Dimensions:       make([]*timestreamwrite.Dimension, len(writeRecord.Dimensions)),
			MeasureName:      aws.String(writeRecord.MeasureName),
			MeasureValue:     aws.String(writeRecord.MeasureValue),
			MeasureValueType: aws.String(writeRecord.MeasureValueType),
			Time:             aws.String(writeRecord.Time),
			TimeUnit:         aws.String(writeRecord.TimeUnit),
			Version:          aws.Int64(writeRecord.Version),
		}
		// add dimensions
		for _, dimension := range writeRecord.Dimensions {
			record.Dimensions = append(record.Dimensions, &timestreamwrite.Dimension{
				Name:  &dimension.Name,
				Value: &dimension.Value,
			})
		}
		// append values to recordsSlice
		recordsSlice = append(recordsSlice, record)
	}

	_, err := ts.w.WriteRecords(&timestreamwrite.WriteRecordsInput{
		DatabaseName: &db,
		TableName:    &table,
		Records:      recordsSlice,
	})
	if err != nil {
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

func ConvertQueryOutput(queryOutput *timestreamquery.QueryOutput) *QueryOutput {
	if queryOutput == nil {
		return nil
	}
	var outputInterface interface{} = *queryOutput
	var result = outputInterface.(QueryOutput)
	return &result
}
