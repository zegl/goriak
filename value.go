package goriak

import (
	"encoding/json"
	"errors"
	"reflect"

	riak "github.com/basho/riak-go-client"
)

// SetValue saves value as key in the bucket bucket/bucketType
// Values can automatically be added to indexes with the struct tag goriakindex
func (c *Client) SetValue(bucket, bucketType, key string, value interface{}) error {
	by, err := json.Marshal(value)

	if err != nil {
		return err
	}

	object := riak.Object{
		Value: by,
	}

	refType := reflect.TypeOf(value)
	refValue := reflect.ValueOf(value)

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

		return errors.New("Did not know how to set index: " + refType.Field(i).Name)
	}

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		WithContent(&object).
		Build()

	if err != nil {
		return err
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err
	}

	res, ok := cmd.(*riak.StoreValueCommand)

	if !ok {
		return errors.New("Unable to parse response from Riak")
	}

	if !res.Success() {
		return errors.New("Riak command was not successful")
	}

	return nil
}

func (c *Client) GetValue(bucket, bucketType, key string, value interface{}) (err error, isNotFound bool) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		Build()

	if err != nil {
		return err, false
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err, false
	}

	res, ok := cmd.(*riak.FetchValueCommand)

	if !ok {
		return errors.New("Unable to parse response from Riak"), false
	}

	if !res.Success() {
		return errors.New("Riak command was not successful"), false
	}

	if res.Response.IsNotFound {
		return errors.New("Not Found"), true
	}

	if len(res.Response.Values) != 1 {
		return errors.New("Not Found"), false
	}

	err = json.Unmarshal(res.Response.Values[0].Value, value)

	if err != nil {
		return err, false
	}

	return nil, false
}

func (c *Client) Delete(bucket, bucketType, key string) error {
	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		Build()

	if err != nil {
		return err
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err
	}

	res, ok := cmd.(*riak.DeleteValueCommand)

	if !ok {
		return errors.New("Could not convert")
	}

	if !res.Success() {
		return errors.New("Command was not successful")
	}

	return nil
}

func (c *Client) AllKeys(bucket, bucketType string) ([]string, error) {
	cmd, err := riak.NewListKeysCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		Build()

	if err != nil {
		return []string{}, err
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return []string{}, err
	}

	res, ok := cmd.(*riak.ListKeysCommand)

	if !ok {
		return []string{}, errors.New("Could not convert")
	}

	return res.Response.Keys, nil
}
