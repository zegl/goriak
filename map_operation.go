package goriak

import (
	riak "github.com/basho/riak-go-client"
)

type commandMapOperation struct {
	*Command
	builder *riak.UpdateMapCommandBuilder
}

// MapOperation takes a riak.MapOperation so that you can run custom commands on your Riak Maps
func (c *Command) MapOperation(op riak.MapOperation, context []byte) *commandMapOperation {
	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithMapOperation(&op)

	if len(context) != 0 {
		builder.WithContext(context)
	}

	return &commandMapOperation{
		Command: c,
		builder: builder,
	}
}

// NewMapOperation returns a new riak.MapOperation that you can for advanced Riak operations
func NewMapOperation() riak.MapOperation {
	return riak.MapOperation{}
}
