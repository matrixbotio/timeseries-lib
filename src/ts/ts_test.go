package ts

import (
	"github.com/aws/aws-sdk-go/service/timestreamquery"
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
	result := convertQueryOutput(&queryOutput)

	// then
	if result == nil {
		t.Errorf("Result should not be nil")
	}
}
