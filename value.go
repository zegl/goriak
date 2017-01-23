package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type allKeysCommand struct {
	*Command
	builder *riak.ListKeysCommandBuilder
}

// AllKeys returns all keys in the set bucket.
// The response will be sent in multiple batches to callback
func (c *Command) AllKeys(callback func([]string) error) *allKeysCommand {
	builder := riak.NewListKeysCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithCallback(callback).
		WithStreaming(true)

	return &allKeysCommand{
		Command: c,
		builder: builder,
	}
}

func (c *allKeysCommand) Run(session *Session) (*Result, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	res := cmd.(*riak.ListKeysCommand)

	if !res.Success() {
		return nil, errors.New("not successful")
	}

	return &Result{}, nil
}
