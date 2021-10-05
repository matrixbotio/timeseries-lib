package logger

import (
	"log"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/matrixbotio/constants-lib"
)

type LogDevice struct {
	es *elasticsearch.Client
}

func (l *LogDevice) Send(data string) {
	if l.es == nil {
		return
	}
	_, err := l.es.Index(
		"logs",
		strings.NewReader(data),
		l.es.Index.WithRefresh("true"),
	)
	if err != nil {
		log.Println("Cannot write to ES: " + err.Error())
	}
}

func getHostname() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return name
}

func createESClient() *elasticsearch.Client {
	host := os.Getenv("ES_HOST")
	if host == "" {
		return nil
	}
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			os.Getenv("ES_PROTO") + "://" + host + ":" + os.Getenv("ES_PORT"),
		},
	})
	if err != nil {
		panic(err)
	}
	return client
}

var Logger = constants.NewLogger(&LogDevice{createESClient()}, getHostname(), "Timeseries worker")
