package goriak

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

// SetJSON saves value as key in the bucket bucket/bucketType
// Values can automatically be added to indexes with the struct tag goriakindex
func (c *Command) SetJSON(value interface{}) *SetRawCommand {
	by, err := json.Marshal(value)

	cmdSet := &SetRawCommand{c: c}

	if err != nil {
		cmdSet.err = err
		return cmdSet
	}

	object := riak.Object{
		Value: by,
	}

	refType := reflect.TypeOf(value)
	refValue := reflect.ValueOf(value)

	// Indexes from struct value
	if refType.Kind() == reflect.Struct {

		// Set indexes
		for i := 0; i < refType.NumField(); i++ {

			indexName := refType.Field(i).Tag.Get("goriakindex")

			if len(indexName) == 0 {
				continue
			}

			switch refValue.Field(i).Type().Kind() {

			// String
			case reflect.String:
				object.AddToIndex(indexName, refValue.Field(i).String())

			// Slice
			case reflect.Slice:

				sliceType := refValue.Field(i).Type().Elem()
				sliceValue := refValue.Field(i)

				switch sliceType.Kind() {

				// []string
				case reflect.String:
					for sli := 0; sli < sliceValue.Len(); sli++ {
						object.AddToIndex(indexName, sliceValue.Index(sli).String())
					}

				// []int
				case reflect.Int:
					fallthrough
				case reflect.Int8:
					fallthrough
				case reflect.Int16:
					fallthrough
				case reflect.Int32:
					fallthrough
				case reflect.Int64:
					for sli := 0; sli < sliceValue.Len(); sli++ {
						object.AddToIndex(indexName, strconv.FormatInt(sliceValue.Index(sli).Int(), 10))
					}

				default:
					cmdSet.err = errors.New("Did not know how to set index: " + refType.Field(i).Name)
					return cmdSet
				}

			// Int
			case reflect.Int:
				fallthrough
			case reflect.Int8:
				fallthrough
			case reflect.Int16:
				fallthrough
			case reflect.Int32:
				fallthrough
			case reflect.Int64:

				// Bashos AddToIntIndex() only accepts int as a type. Using AddToIndex() has
				// the same effect but allows for different sizes of ints
				object.AddToIndex(
					indexName,
					strconv.FormatInt(refValue.Field(i).Int(), 10),
				)

			default:
				cmdSet.err = errors.New("Did not know how to set index: " + refType.Field(i).Name)
				return cmdSet
			}
		}
	}

	builder := riak.NewStoreValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	cmdSet.storeValueObject = &object
	cmdSet.storeValueCommandBuilder = builder

	return cmdSet
}

// GetJSON is the same as GetRaw, but with automatic JSON unmarshalling
func (c *Command) GetJSON(key string, output interface{}) *GetRawCommand {
	builder := riak.NewFetchValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	return &GetRawCommand{
		c:   c,
		key: key,

		bucket:     c.bucket,
		bucketType: c.bucketType,

		builder: builder,
		output:  output,
	}
}
