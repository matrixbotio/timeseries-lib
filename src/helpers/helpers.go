package helpers

import (
	"_/src/structs"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
)

func fromMapStr(mapObj map[string]interface{}, prop string, dest *string) bool {
	val, ok := mapObj[prop].(string)
	if !ok {
		return false
	}
	*dest = val
	return true
}

func fromMapInt64(mapObj map[string]interface{}, prop string, dest *int64) bool {
	val, ok := mapObj[prop].(int64)
	if !ok {
		return false
	}
	*dest = val
	return true
}

func ConvertRecords(unconverted interface{}) ([]*structs.WriteRecord, bool) {
	ifaceArr, ok := unconverted.([]interface{})
	if !ok {
		return nil, false
	}
	converted := make([]*structs.WriteRecord, len(ifaceArr))
	for i, v := range ifaceArr {
		convertedMap, ok := v.(map[string]interface{})
		if !ok {
			return nil, false
		}
		dimensionsArr, isDimensionsOk := convertedMap["dimensions"].([]interface{})
		if !isDimensionsOk {
			return nil, false
		}
		dimensions := make([]structs.RecordDimension, len(dimensionsArr))
		for i, dimIface := range dimensionsArr {
			dimMap, ok := dimIface.(map[string]interface{})
			if !ok {
				return nil, false
			}
			dimensions[i] = structs.RecordDimension{}
			success := fromMapStr(dimMap, "name", &dimensions[i].Name) && fromMapStr(dimMap, "value", &dimensions[i].Value)
			if !success {
				return nil, false
			}
		}
		convertedValue := &structs.WriteRecord{
			Dimensions: dimensions,
		}
		success := fromMapInt64(convertedMap, "version", &convertedValue.Version)
		for name, remapTo := range map[string]*string{
			"measureName":  &convertedValue.MeasureName,
			"measureValue": &convertedValue.MeasureValue,
			"measureType":  &convertedValue.MeasureValueType,
			"time":         &convertedValue.Time,
			"timeUnit":     &convertedValue.TimeUnit,
		} {
			success = success && fromMapStr(convertedMap, name, remapTo)
		}
		if !success {
			return nil, false
		}
		converted[i] = convertedValue
	}
	return converted, true
}

func ConvertQueryOutput(queryOutput *timestreamquery.QueryOutput) *structs.QueryOutput {
	var columnInfo []*structs.ColumnInfo
	for i := range queryOutput.ColumnInfo {
		tsColumnInfo := queryOutput.ColumnInfo[i]
		columnInfo = append(columnInfo, &structs.ColumnInfo{
			Name: tsColumnInfo.Name,
			Type: tsColumnInfo.Type.ScalarType,
		})
	}
	var rows []*structs.Row
	for i := range queryOutput.Rows {
		tsRow := queryOutput.Rows[i]
		var data []*string
		for j := range tsRow.Data {
			tsRowData := tsRow.Data[j]
			data = append(data, tsRowData.ScalarValue)
		}
		rows = append(rows, &structs.Row{Data: data})
	}
	return &structs.QueryOutput{
		ColumnInfo: columnInfo,
		NextToken:  queryOutput.NextToken,
		Rows:       rows,
	}
}
