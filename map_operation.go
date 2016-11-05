package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// MapOperation takes a riak.MapOperation so that you can run custom commands on your Riak Maps
func (c Command) MapOperation(op riak.MapOperation, context []byte) Command {
	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithMapOperation(&op).
		WithContext(context)

	c.updateMapCommandBuilder = builder
	c.commandType = riakUpdateMapCommand

	return c
}

// NewMapOperation returns a new riak.MapOperation that you can for advanced Riak operations
func NewMapOperation() riak.MapOperation {
	return riak.MapOperation{}
}
