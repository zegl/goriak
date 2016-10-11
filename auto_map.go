package goriak

import (
	"errors"
	"reflect"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

func (c *Client) SetMap(bucket, bucketType, key string, input interface{}) error {
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

		// Int -> Register
		if field.Type.Kind() == reflect.Int {
			op.SetRegister(itemKey, []byte(strconv.Itoa(int(rValue.Field(i).Int()))))
			continue
		}

		// String -> Register
		if field.Type.Kind() == reflect.String {
			op.SetRegister(itemKey, []byte(rValue.Field(i).String()))
			continue
		}

		// Array -> Register
		if field.Type.Kind() == reflect.Array {

			f := rValue.Field(i)

			// Empty
			if f.Len() == 0 {
				op.SetRegister(itemKey, []byte{})
				continue
			}

			// Byte array (uint8 is the same as byte)
			if f.Index(0).Kind() == reflect.Uint8 {
				register := make([]byte, f.Len())

				for ii := 0; ii < f.Len(); ii++ {
					register[ii] = uint8(f.Index(ii).Uint())
				}

				op.SetRegister(itemKey, register)

				continue
			}

			return errors.New("Unkown Array type: " + f.Index(0).Kind().String())
		}

		// Slice -> Set
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

			// Slice: Uint8 (byte)
			if rType.Field(i).Type.Elem().Kind() == reflect.Uint8 {
				op.SetRegister(itemKey, sliceVal.Bytes())
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

	err = c.riak.Execute(cmd)

	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetMap(bucket, bucketType, key string, output interface{}) (err error, isNotFound bool) {

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

	err = c.riak.Execute(cmd)

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

		// Array
		if field.Type.Kind() == reflect.Array {

			// []byte
			if rValue.Field(i).Type().Elem().Kind() == reflect.Uint8 {
				if val, ok := data.Registers[registerName]; ok {
					for ii := 0; ii < rValue.Field(i).Len(); ii++ {
						rValue.Field(i).Index(ii).SetUint(uint64(val[ii]))
					}
				}
			}

			continue
		}

		// Int
		if field.Type.Kind() == reflect.Int {
			if val, ok := data.Registers[registerName]; ok {
				intVal, err := strconv.Atoi(string(val))

				if err == nil {
					rValue.Field(i).SetInt(int64(intVal))
				}
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

			// Slice: Uint8 (byte)
			if rValue.Field(i).Type().Elem().Kind() == reflect.Uint8 {
				if val, ok := data.Registers[registerName]; ok {
					rValue.Field(i).SetBytes(val)
				}

				continue
			}

			return errors.New("Unknown slice type: " + rValue.Field(i).Type().Elem().Kind().String()), false
		}

		return errors.New("Unknown type: " + field.Type.Kind().String()), false
	}

	return nil, false
}

func (c *Client) MapOperation(bucket, bucketType, key string, op riak.MapOperation) error {
	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		WithMapOperation(&op).
		Build()

	if err != nil {
		return err
	}

	err = c.riak.Execute(cmd)

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
