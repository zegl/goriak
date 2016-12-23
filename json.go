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
func (c Command) SetJSON(value interface{}) Command {
	by, err := json.Marshal(value)

	if err != nil {
		c.err = err
		return c
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
					c.err = errors.New("Did not know how to set index: " + refType.Field(i).Name)
					return c
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
				c.err = errors.New("Did not know how to set index: " + refType.Field(i).Name)
				return c
			}
		}
	}

	builder := riak.NewStoreValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	c.storeValueObject = &object
	c.storeValueCommandBuilder = builder

	// c.riakCommand = cmd
	c.commandType = riakStoreValueCommand

	return c
}

// GetJSON is the same as GetRaw, but with automatic JSON unmarshalling
func (c Command) GetJSON(key string, output interface{}) Command {
	builder := riak.NewFetchValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	c.key = key
	c.fetchValueCommandBuilder = builder
	c.commandType = riakFetchValueCommandJSON
	c.output = output

	return c
}
