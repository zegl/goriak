package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// Delete deletes the value set by Key()
func (c Command) Delete() Command {
	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(c.key).
		Build()

	if err != nil {
		c.err = err
		return c
	}

	c.commandType = riakDeleteValueCommand
	c.riakCommand = cmd

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
