package goriak

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	riak "github.com/basho/riak-go-client"
)

func TsQuery(query string, session *Session) error {
	cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(query).Build()
	if err != nil {
		return err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return err
	}

	if !cmd.Success() {
		return errors.New("TsQuery failed")
	}

	return nil
}

func TsWrite(table string, object interface{}, session *Session) error {
	// Map TsCell ID to Riak struct Field
	cellIDs := make(map[int]int)

	var maxID int

	r := reflect.TypeOf(object)

	num := r.NumField()
	for i := 0; i < num; i++ {
		tag := r.Field(i).Tag.Get("goriakts")
		tagPiece := strings.Split(tag, ",")

		if len(tagPiece) != 2 {
			return errors.New("Unexpected goriakts tag")
		}

		tagNum, err := strconv.Atoi(tagPiece[0])
		if err != nil {
			return errors.New("Unexpected goriakts tag")
		}

		cellIDs[tagNum] = i

		// Keep track of the largest num found
		if tagNum > maxID {
			maxID = tagNum
		}
	}

	row := make([]riak.TsCell, maxID+1)

	rVal := reflect.ValueOf(object)

	for tsCellID, fieldID := range cellIDs {
		f := rVal.Field(fieldID)

		switch r.Field(fieldID).Type.Kind() {
		case reflect.String:
			row[tsCellID] = riak.NewStringTsCell(f.String())
		case reflect.Int64:
			row[tsCellID] = riak.NewSint64TsCell(f.Int())
		case reflect.Struct:
			if ts, ok := f.Interface().(time.Time); ok {
				row[tsCellID] = riak.NewTimestampTsCell(ts)
			} else {
				return errors.New("Unknown Type in TsWrite: " + r.Field(fieldID).Type.Kind().String())
			}
		default:
			return errors.New("Unknown Type in TsWrite: " + r.Field(fieldID).Type.Kind().String())
		}
	}

	cmd, err := riak.NewTsStoreRowsCommandBuilder().
		WithTable(table).
		WithRows([][]riak.TsCell{row}).
		Build()
	if err != nil {
		return err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return err
	}

	if !cmd.Success() {
		return errors.New("TsWrite command execute failed")
	}

	return nil
}

func TsTimeFormat(in time.Time) int64 {
	return riak.ToUnixMillis(in)
}

func TsRead(query string, objects interface{}, session *Session) error {

	ptrValue := reflect.ValueOf(objects)
	ptrOuterType := reflect.TypeOf(objects)

	if ptrOuterType.Kind() != reflect.Ptr {
		return errors.New("objects is expected to be of ptr slice struct type (outer was not slice). Was: " + ptrOuterType.Kind().String())
	}

	sliceOuterType := ptrOuterType.Elem()
	sliceValue := ptrValue.Elem()
	if sliceOuterType.Kind() != reflect.Slice {
		return errors.New("objects is expected to be of ptr slice struct type (outer was not slice). Was: " + sliceOuterType.Kind().String())
	}

	innerType := ptrOuterType.Elem().Elem()
	if innerType.Kind() != reflect.Struct {
		return errors.New("objects is expected to be of slice struct type (slice type was not struct)")
	}

	// Map field names (from the goriakts tag) to their field position
	fieldNameToPos := make(map[string]int)
	numField := innerType.NumField()

	for i := 0; i < numField; i++ {
		tag := innerType.Field(i).Tag.Get("goriakts")
		tagPieces := strings.Split(tag, ",")

		if len(tagPieces) != 2 {
			return errors.New("Unexpected value of the goriakts tag on " + innerType.String() + ":" + innerType.Field(i).Name)
		}

		fieldNameToPos[tagPieces[1]] = i
	}

	cmd, err := riak.NewTsQueryCommandBuilder().WithQuery(query).Build()
	if err != nil {
		return err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return err
	}

	res := cmd.(*riak.TsQueryCommand)

	if !res.Success() {
		return errors.New("Query was not successfully executed")
	}

	// Create a new slice
	outputSlice := reflect.MakeSlice(sliceOuterType, len(res.Response.Rows), len(res.Response.Rows))

	for rowID, row := range res.Response.Rows {

		// Create a new struct
		newOutObject := reflect.New(innerType).Elem()

		for colID, data := range row {
			colName := res.Response.Columns[colID].GetName()
			if pos, ok := fieldNameToPos[colName]; ok {
				switch data.GetDataType() {
				case "VARCHAR":
					newOutObject.Field(pos).SetString(data.GetStringValue())
				case "SINT64":
					newOutObject.Field(pos).SetInt(data.GetSint64Value())
				case "TIMESTAMP":
					newOutObject.Field(pos).Set(reflect.ValueOf(data.GetTimeValue()))
				default:
					return errors.New("Unknown type " + data.GetDataType() + " in result parsing")
				}
			} else {
				return errors.New("Could not map " + colName + " to output object")
			}
		}

		outputSlice.Index(rowID).Set(newOutObject)
	}

	// Replace the user subbmited slice with the one that we have created
	sliceValue.Set(outputSlice)

	return nil
}

func TsDel() {
	riak.NewTsDeleteRowCommandBuilder()
}
