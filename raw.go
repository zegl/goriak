package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// SetRaw allows you to set a []byte directly to Riak.
// SetRaw gives you full control of the data stored, compared to SetJSON and Set.
func (c Command) SetRaw(value []byte) Command {
	object := riak.Object{
		Value: value,
	}

	builder := riak.NewStoreValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	c.storeValueObject = &object
	c.storeValueCommandBuilder = builder
	c.commandType = riakStoreValueCommand

	return c
}

// GetRaw retreives key as a []byte.
// The output will be written to output by Run().
func (c Command) GetRaw(key string, output *[]byte) Command {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key).
		Build()

	if err != nil {
		c.err = err
		return c
	}

	c.key = key

	c.riakCommand = cmd
	c.commandType = riakFetchValueCommandRaw
	c.outputBytes = output

	return c
}
