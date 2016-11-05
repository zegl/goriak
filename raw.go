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

	// Add to indexes
	for indexName, values := range c.indexes {
		for _, val := range values {
			object.AddToIndex(indexName, val)
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
