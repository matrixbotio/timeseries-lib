package ts

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/service/timestreamwrite"
    "github.com/aws/aws-sdk-go/service/timestreamquery"
	"net/http"
	"golang.org/x/net/http2"
	"time"
	"net"
	"os"
)

type TS struct {
	q *timestreamquery.TimestreamQuery
	w *timestreamwrite.TimestreamWrite
}

func createTSSession() (*timestreamquery.TimestreamQuery, *timestreamwrite.TimestreamWrite) {
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			DualStack: true,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	http2.ConfigureTransport(tr)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("AWS_REGION")),
		Credentials: credentials.NewStaticCredentials(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), ""),
		MaxRetries: aws.Int(10),
		HTTPClient: &http.Client{ Transport: tr },
	})

	if err != nil {
		panic("Failed to create AWS session: " + err.Error())
	}

	return timestreamquery.New(sess), timestreamwrite.New(sess)
}

func New() *TS {
	q, w := createTSSession()
	return &TS{
		q: q,
		w: w,
	}
}

func (ts *TS) Query(query string) (interface{}, error) {
	ts.q.Query(&timestreamquery.QueryInput{
		QueryString: &query,
	})
}

type WriteRecord struct{
	//
}

func (ts *TS) Write(db, table string, records []*WriteRecord) error {
	recordsSlice := make([]*timestreamwrite.Record, 0)
	for _, x := range records {
		// append values to recordsSlice
	}

	ts.w.WriteRecords(&timestreamwrite.WriteRecordsInput{
		DatabaseName: &db,
		TableName: &table,
		Records: recordsSlice,
	})
}
