package ts

import (
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
)

// TS - TS handler struct
type TS struct {
	q *timestreamquery.TimestreamQuery
	w *timestreamwrite.TimestreamWrite
}

// QueryOutput - output of the TS query
type QueryOutput struct {
	_           struct{}      `json:"-"`
	ColumnInfo  []*ColumnInfo `json:"columnInfo,omitempty"`
	NextToken   *string       `json:"nextToken,omitempty"`
	QueryId     *string       `json:"queryId,omitempty"`
	QueryStatus *QueryStatus  `json:"queryStatus,omitempty"`
	Rows        []*Row        `json:"rows,omitempty"`
}

// ColumnInfo - Contains the meta data for query results such as the column names, data types,
// and other attributes.
type ColumnInfo struct {
	_    struct{} `json:"-"`
	Name *string  `json:"name,omitempty"`
	Type *Type    `json:"type,omitempty"`
}

// QueryStatus - Information about the status of the query, including progress and bytes scannned.
type QueryStatus struct {
	_                      struct{} `json:"-"`
	CumulativeBytesMetered *int64   `json:"cumulativeBytesMetered,omitempty"`
	CumulativeBytesScanned *int64   `json:"cumulativeBytesScanned,omitempty"`
	ProgressPercentage     *float64 `json:"progressPercentage,omitempty"`
}

// Row - Represents a single row in the query results.
type Row struct {
	_    struct{} `json:"-"`
	Data []*Datum `json:"data,omitempty"`
}

// Type - Contains the data type of a column in a query result set. The data type can
// be scalar or complex. The supported scalar data types are integers, boolean,
// string, double, timestamp, date, time, and intervals. The supported complex
// data types are arrays, rows, and timeseries.
type Type struct {
	_                                struct{}      `json:"-"`
	ArrayColumnInfo                  *ColumnInfo   `json:"arrayColumnInfo,omitempty"`
	RowColumnInfo                    []*ColumnInfo `json:"rowColumnInfo,omitempty"`
	ScalarType                       *string       `json:"scalarType,omitempty"`
	TimeSeriesMeasureValueColumnInfo *ColumnInfo   `json:"timeSeriesMeasureValueColumnInfo,omitempty"`
}

// Datum - represents a single data point in a query result.
type Datum struct {
	_               struct{}               `json:"-"`
	ArrayValue      []*Datum               `json:"arrayValue,omitempty"`
	NullValue       *bool                  `json:"nullValue,omitempty"`
	RowValue        *Row                   `json:"rowValue,omitempty"`
	ScalarValue     *string                `json:"scalarValue,omitempty"`
	TimeSeriesValue []*TimeSeriesDataPoint `json:"timeSeriesValue,omitempty"`
}

// TimeSeriesDataPoint - The timeseries datatype represents the values of a measure over time. A time
// series is an array of rows of timestamps and measure values, with rows sorted
// in ascending order of time. A TimeSeriesDataPoint is a single data point
// in the timeseries. It represents a tuple of (time, measure value) in a timeseries.
type TimeSeriesDataPoint struct {
	_     struct{} `json:"-"`
	Time  *string  `json:"time,omitempty"`
	Value *Datum   `json:"value,omitempty"`
}

// WriteRecord - TS Record data container
type WriteRecord struct {
	Dimensions       []RecordDimension `json:"dimensions"`
	MeasureName      string            `json:"measureName"`
	MeasureValue     string            `json:"measureValue"`
	MeasureValueType string            `json:"measureType"` // example: "DOUBLE"
	Time             string            `json:"time"`
	TimeUnit         string            `json:"timeUnit"` // example: "MILLISECONDS"
	Version          int64             `json:"version"`
}

// RecordDimension - ts record dimension
type RecordDimension struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
