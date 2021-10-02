package logger

import (
	"os"
	"strings"

	"github.com/matrixbotio/constants-lib"
	"github.com/elastic/go-elasticsearch/v7"
)

type LogDevice struct {
	es *elasticsearch.Client
}

func (l *LogDevice) Send(data string){
	l.es.Index(
		"logs",
		strings.NewReader(data),
		l.es.Index.WithRefresh("true"),
	)
}

func getHostname() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return name
}

func createESClient() *elasticsearch.Client {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			os.Getenv("ES_PROTO") + "://" + os.Getenv("ES_HOST") + ":" + os.Getenv("ES_PORT"),
		},
	})
	if err != nil {
		panic(err)
	}
	return client
}

var Logger = constants.NewLogger(&LogDevice{createESClient()}, getHostname(), "Timeseries worker")
