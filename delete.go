package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type DeleteCommand struct {
	*Command
	builder *riak.DeleteValueCommandBuilder
}

// Delete deletes the value stored as key
func (c *Command) Delete(key string) *DeleteCommand {
	builder := riak.NewDeleteValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	return &DeleteCommand{
		Command: c,
		builder: builder,
	}
}

func (c *DeleteCommand) Run(session *Session) (*Result, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	res := cmd.(*riak.DeleteValueCommand)

	if !res.Success() {
		return nil, errors.New("not successful")
	}

	return &Result{}, nil
}
