package goriak

import (
	riak "github.com/basho/riak-go-client"
)

func (c Command) SetRaw(value []byte) Command {
	object := riak.Object{
		Value: value,
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

func (c Command) GetRaw(output *[]byte) Command {
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
	c.commandType = riakFetchValueCommandRaw
	c.outputBytes = output

	return c
}
