package structs

// QueryOutput - output of the TS query
type QueryOutput struct {
	ColumnInfo []ColumnInfo `json:"columnInfo,omitempty"`
	NextToken  string       `json:"nextToken,omitempty"`
	Rows       []Row        `json:"rows,omitempty"`
}

// ColumnInfo - Contains the meta data for query results such as the column names, data types,
// and other attributes.
type ColumnInfo struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
}

// Row - Represents a single row in the query results.
type Row struct {
	Data []string `json:"data,omitempty"`
}

// WriteRecord - TS Record data container
type WriteRecord struct {
	Dimensions       []RecordDimension `json:"dimensions"`
	MeasureName      string            `json:"measureName"`
	MeasureValue     string            `json:"measureValue"`
	MeasureValueType string            `json:"measureType"` // example: "DOUBLE"
	Time             string            `json:"time"`
	TimeUnit         string            `json:"timeUnit"` // example: "MILLISECONDS"
	Version          float64           `json:"version"`
}

// RecordDimension - ts record dimension
type RecordDimension struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
