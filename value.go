package goriak

import (
	riak "github.com/basho/riak-go-client"
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
