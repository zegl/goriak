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

/*
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
