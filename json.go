package goriak

import (
	"encoding/json"
	"errors"
	"reflect"

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

	if refType.Kind() == reflect.Struct {

		// Set indexes
		for i := 0; i < refType.NumField(); i++ {

			indexName := refType.Field(i).Tag.Get("goriakindex")

			if len(indexName) == 0 {
				continue
			}

			// String
			if refValue.Field(i).Type().Kind() == reflect.String {
				object.AddToIndex(indexName, refValue.Field(i).String())
				continue
			}

			// Slice
			if refValue.Field(i).Type().Kind() == reflect.Slice {

				sliceType := refValue.Field(i).Type().Elem()
				sliceValue := refValue.Field(i)

				// Slice: String
				if sliceType.Kind() == reflect.String {
					for sli := 0; sli < sliceValue.Len(); sli++ {
						object.AddToIndex(indexName, sliceValue.Index(sli).String())
					}

					continue
				}
			}

			c.err = errors.New("Did not know how to set index: " + refType.Field(i).Name)
			return c
		}

	}

	builder := riak.NewStoreValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithContent(&object)

	if c.key != "" {
		builder = builder.WithKey(c.key)
	}

	cmd, err := builder.Build()

	if err != nil {
		c.err = err
		return c
	}

	c.riakCommand = cmd
	c.commandType = riakStoreValueCommand

	return c
}

// GetJSON is the same as GetRaw, but with automatic JSON unmarshalling
func (c Command) GetJSON(output interface{}) Command {
	// func (c *Client) GetJSON(bucket, bucketType, key string, value interface{}) (err error, isNotFound bool) {
	// raw, err, isNotFound := c.GetRaw(bucket, bucketType, key)

	/*if err != nil {
		return err, isNotFound
	}

	err = json.Unmarshal(raw, value)

	if err != nil {
		return err, false
	}

	return nil, false*/

	return c.GetRaw(output)

}

func (c Command) GetRaw(output interface{}) Command {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(c.key).
		Build()

	if err != nil {
		c.err = err
		return c
	}

	c.riakCommand = cmd
	c.commandType = riakFetchValueCommand
	c.output = output

	return c
}
