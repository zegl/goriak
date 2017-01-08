package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// Delete deletes the value stored as key
func (c Command) Delete(key string) Command {
	builder := riak.NewDeleteValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	c.key = key
	c.deleteValueCommandBuilder = builder
	c.commandType = riakDeleteValueCommand

	return c
}

// AllKeys returns all keys in the set bucket.
// The response will be sent in multiple batches to callback
func (c Command) AllKeys(callback func([]string) error) Command {
	cmd, err := riak.NewListKeysCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithCallback(callback).
		WithStreaming(true).
		Build()

	if err != nil {
		c.err = err
		return c
	}

	c.commandType = riakListKeysCommand
	c.riakCommand = cmd

	return c
}
