package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type MapOperationCommand struct {
	c       *Command
	builder *riak.UpdateMapCommandBuilder
}

// MapOperation takes a riak.MapOperation so that you can run custom commands on your Riak Maps
func (c *Command) MapOperation(op riak.MapOperation) *MapOperationCommand {
	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithMapOperation(&op)

	return &MapOperationCommand{
		c:       c,
		builder: builder,
	}
}

func (c *MapOperationCommand) Context(ctx []byte) *MapOperationCommand {
	c.builder.WithContext(ctx)
	return c
}

func (c *MapOperationCommand) Key(key string) *MapOperationCommand {
	c.builder.WithKey(key)
	return c
}

func (c *MapOperationCommand) Run(session *Session) (*Result, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	res := cmd.(*riak.UpdateMapCommand)

	if !res.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}

// NewMapOperation returns a new riak.MapOperation that you can for advanced Riak operations
func NewMapOperation() riak.MapOperation {
	return riak.MapOperation{}
}
