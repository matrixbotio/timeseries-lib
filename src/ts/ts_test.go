package ts

import (
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"testing"
)

func TestConvertQueryOutput(t *testing.T) {
	// given
	nextToken := "mockedNextToken"
	queryId := "mockedQueryId"
	queryOutput := timestreamquery.QueryOutput{
		NextToken: &nextToken,
		QueryId:   &queryId,
	}

	// when
	result := ConvertQueryOutput(&queryOutput)

	// then
	if result == nil {
		t.Errorf("Result should not be nil")
	}
}
