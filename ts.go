package timeseries

import (
	"errors"
	"fmt"
	"github.com/matrixbotio/constants-lib"
	"github.com/matrixbotio/timeseries-lib/helpers"
	"github.com/matrixbotio/timeseries-lib/structs"
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

var log *constants.Logger

// TS - TS handler struct
type TS struct {
	q *timestreamquery.TimestreamQuery
	w *timestreamwrite.TimestreamWrite
}

// New - create new TS handler
func New(logger *constants.Logger, createWriteClient bool, createQueryClient bool) (*TS, helpers.ApiError) {
	if !createWriteClient && !createQueryClient {
		return &TS{}, constants.Error(helpers.BaseInternalError, "No ts clients were selected for creation")
	}

	if log == nil {
		if logger == nil {
			return nil, constants.Error(helpers.BaseInternalError, "Logger should be passed")
		}
		log = logger
	}

	var q *timestreamquery.TimestreamQuery = nil
	if createQueryClient {
		queryEndpoint := os.Getenv("TS_QUERY_ENDPOINT")
		sess, err := createTSSession(queryEndpoint)
		if err != nil {
			return nil, err
		}
		q = timestreamquery.New(sess)
	}

	var w *timestreamwrite.TimestreamWrite = nil
	if createWriteClient {
		writeEndpoint := os.Getenv("TS_WRITE_ENDPOINT")
		sess, err := createTSSession(writeEndpoint)
		if err != nil {
			return nil, err
		}
		w = timestreamwrite.New(sess)
	}

	return &TS{
		q: q,
		w: w,
	}, nil
}

// Query - select ts data
func (ts *TS) Query(query string, nextToken string) (*structs.QueryOutput, error) {
	var nt *string
	if nextToken == "" {
		nt = nil
	} else {
		nt = &nextToken
	}
	tsResult, err := ts.q.Query(&timestreamquery.QueryInput{
		QueryString: &query,
		NextToken:   nt,
	})
	if err != nil {
		return nil, errors.New("failed to exec ts query: " + err.Error())
	}
	return helpers.ConvertQueryOutput(tsResult), nil
}

// Write ts records
func (ts *TS) Write(db, table string, records []*structs.WriteRecord) error {
	writeRecordsInput := helpers.ConvertWriteRecordsInput(db, table, records)
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

// createTSSession - create TS session
func createTSSession(endpoint string) (*session.Session, helpers.ApiError) {
	accessKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
	if accessKeyId == "" {
		return nil, constants.Error(helpers.BaseInternalError,
			"AWS_ACCESS_KEY_ID env variable should be set")
	}
	accessSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessSecretKey == "" {
		return nil, constants.Error(helpers.BaseInternalError,
			"AWS_SECRET_ACCESS_KEY env variable should be set")
	}

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
		return nil, constants.Error(helpers.BaseInternalError, "Failed to configure http2 transport: "+err.Error())
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "eu-west-1"
	}
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyId, accessSecretKey, ""),
		MaxRetries:  aws.Int(10),
		HTTPClient:  &http.Client{Transport: tr},
		Endpoint:    &endpoint,
	})

	if err != nil {
		return nil, constants.Error(helpers.BaseInternalError, "Failed to create AWS session: "+err.Error())
	}

	return sess, nil
}
