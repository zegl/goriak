package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// SetRaw allows you to set a []byte directly to Riak.
// SetRaw gives you full control of the data stored, compared to SetJSON and Set.
func (c *Command) SetRaw(value []byte) *commandSet {
	object := &riak.Object{
		Value: value,
	}

	builder := riak.NewStoreValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	return &commandSet{
		Command:                  c,
		storeValueObject:         object,
		storeValueCommandBuilder: builder,
	}
}

// GetRaw retreives key as a []byte.
// The output will be written to output by Run().
func (c *Command) GetRaw(key string, output *[]byte) *Command {
	builder := riak.NewFetchValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	c.fetchValueCommandBuilder = builder
	c.key = key
	c.commandType = riakFetchValueCommandRaw
	c.outputBytes = output

	return c
}
