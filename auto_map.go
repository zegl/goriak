package goriak

import (
	"errors"
	"reflect"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

func SetMap(bucket, bucketType, key string, input interface{}) error {
	op := riak.MapOperation{}

	var rValue reflect.Value
	var rType reflect.Type

	if reflect.ValueOf(input).Kind() == reflect.Struct {
		rValue = reflect.ValueOf(input)
		rType = reflect.TypeOf(input)
	} else if reflect.ValueOf(input).Kind() == reflect.Ptr {
		rValue = reflect.ValueOf(input).Elem()
		rType = reflect.TypeOf(input).Elem()
	} else {
		return errors.New("Could not parse value. Needs to be struct or pointer to struct")
	}

	num := rType.NumField()

	for i := 0; i < num; i++ {
		field := rType.Field(i)

		itemKey := field.Name

		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			itemKey = tag
		}

		// Register: String
		if field.Type.Kind() == reflect.String {
			op.SetRegister(itemKey, []byte(rValue.Field(i).String()))
			continue
		}

		// Set
		if field.Type.Kind() == reflect.Slice {

			sliceLength := rValue.Field(i).Len()
			sliceVal := rValue.Field(i).Slice(0, sliceLength)

			// Slice: Int
			if rType.Field(i).Type.Elem().Kind() == reflect.Int {

				// Convert Int -> String -> []byte
				for ii := 0; ii < sliceLength; ii++ {
					intVal := sliceVal.Index(ii).Int()
					strVal := strconv.FormatInt(intVal, 10)
					op.AddToSet(itemKey, []byte(strVal))
				}

				continue
			}

			// Slice: String
			if rType.Field(i).Type.Elem().Kind() == reflect.String {

				for ii := 0; ii < sliceLength; ii++ {
					strVal := sliceVal.Index(ii).String()
					op.AddToSet(itemKey, []byte(strVal))
				}

				continue
			}

			return errors.New("Unknown slice type: " + sliceVal.Index(0).Type().String())
		}

		return errors.New("Unexpected type: " + field.Type.Kind().String())
	}

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(bucket).
		WithKey(key).
		WithBucketType(bucketType).
		WithMapOperation(&op).
		Build()

	if err != nil {
		return err
	}

	err = connect().Execute(cmd)

	if err != nil {
		return err
	}

	return nil
}

func GetMap(bucket, bucketType, key string, output interface{}) (err error, isNotFound bool) {

	// Type check
	if reflect.ValueOf(output).Kind() != reflect.Ptr {
		return errors.New("output needs to be a pointer"), false
	}

	cmd, err := riak.NewFetchMapCommandBuilder().
		WithBucket(bucket).
		WithKey(key).
		WithBucketType(bucketType).
		Build()

	if err != nil {
		return err, false
	}

	err = connect().Execute(cmd)

	if err != nil {
		return err, false
	}

	ma := cmd.(*riak.FetchMapCommand)

	if !ma.Success() {
		return errors.New("Not successful"), false
	}

	if ma.Response.IsNotFound {
		return errors.New("Not found"), true
	}

	data := ma.Response.Map

	// Set values
	rValue := reflect.ValueOf(output).Elem()
	rType := reflect.TypeOf(output).Elem()
	num := rType.NumField()

	for i := 0; i < num; i++ {

		field := rType.Field(i)
		registerName := field.Name
		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			registerName = tag
		}

		// String
		if field.Type.Kind() == reflect.String {
			if val, ok := data.Registers[registerName]; ok {
				rValue.Field(i).SetString(string(val))
			}

			continue
		}

		// Slice
		if field.Type.Kind() == reflect.Slice {

			// Slice: Int
			if rValue.Field(i).Type().Elem().Kind() == reflect.Int {
				if setVal, ok := data.Sets[registerName]; ok {
					result := make([]int, len(setVal))

					for i, v := range setVal {
						intVal, err := strconv.ParseInt(string(v), 10, 64)

						if err != nil {
							panic(err)
						}

						result[i] = int(intVal)
					}

					// Success!
					rValue.Field(i).Set(reflect.ValueOf(result))
				}

				continue
			}

			// Slice: String
			if rValue.Field(i).Type().Elem().Kind() == reflect.String {
				if setVal, ok := data.Sets[registerName]; ok {
					result := make([]string, len(setVal))

					for i, v := range setVal {
						result[i] = string(v)
					}

					// Success!
					rValue.Field(i).Set(reflect.ValueOf(result))
				}

				continue
			}

			return errors.New("Unknown slice type: " + rValue.Field(i).Type().Elem().Kind().String()), false
		}

		return errors.New("Unknown type: " + field.Type.Kind().String()), false
	}

	return nil, false
}

func MapOperation(bucket, bucketType, key string, op riak.MapOperation) error {
	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		WithMapOperation(&op).
		Build()

	if err != nil {
		return err
	}

	err = connect().Execute(cmd)

	if err != nil {
		return err
	}

	res, ok := cmd.(*riak.UpdateMapCommand)

	if !ok {
		return errors.New("Could not convert")
	}

	if !res.Success() {
		return errors.New("Not successful")
	}

	return nil
}

func NewMapOperation() riak.MapOperation {
	return riak.MapOperation{}
}
