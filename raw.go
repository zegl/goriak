package goriak

import (
	riak "gopkg.in/zegl/goriak.v3/deps/riak-go-client"
)

// SetRaw allows you to set a []byte directly to Riak.
// SetRaw gives you full control of the data stored, compared to SetJSON and Set.
func (c *Command) SetRaw(value []byte) *SetRawCommand {

	object := &riak.Object{
		Value: value,
	}

	builder := riak.NewStoreValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	return &SetRawCommand{
		c:                        c,
		storeValueObject:         object,
		storeValueCommandBuilder: builder,
	}
}

// GetRaw retreives key as a []byte.
// The output will be written to output by Run().
func (c *Command) GetRaw(key string, output *[]byte) *GetRawCommand {
	cmd := &GetRawCommand{
		c: c,

		bucket:     c.bucket,
		bucketType: c.bucketType,

		key:         key,
		outputBytes: output,
		isRawOutput: true,
	}

	cmd.builder = riak.NewFetchValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	return cmd
}
