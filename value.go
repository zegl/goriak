package goriak

import (
// "errors"

//riak "github.com/basho/riak-go-client"
)

type Options struct {
	indexes map[string][]string
}

func (o *Options) AddToIndex(key, value string) *Options {

	// Create map if needed
	if o.indexes == nil {
		o.indexes = make(map[string][]string)
	}

	// Add to existing slice
	if _, ok := o.indexes[key]; ok {
		o.indexes[key] = append(o.indexes[key], value)
		return o
	}

	// Create new slice
	o.indexes[key] = []string{value}
	return o
}

// SetRaw saves the data in Riak directly without any modifications
/*func (c *Client) SetRaw(bucket, bucketType, key string, data []byte, opt *Options) error {
	object := riak.Object{
		Value: data,
	}

	// Add to indexes
	if opt != nil {
		for name, values := range opt.indexes {
			for _, val := range values {
				object.AddToIndex(name, val)
			}
		}
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

// GetRaw retuns the raw []byte array that is stored in Riak without any modifications
func (c *Client) GetRaw(bucket, bucketType, key string) (raw []byte, err error, isNotFound bool) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		Build()

	if err != nil {
		return raw, err, false
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return raw, err, false
	}

	res, ok := cmd.(*riak.FetchValueCommand)

	if !ok {
		return raw, errors.New("Unable to parse response from Riak"), false
	}

	if !res.Success() {
		return raw, errors.New("Riak command was not successful"), false
	}

	if res.Response.IsNotFound {
		return raw, errors.New("Not Found"), true
	}

	if len(res.Response.Values) != 1 {
		return raw, errors.New("Not Found"), false
	}

	return res.Response.Values[0].Value, nil, false
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
*/
