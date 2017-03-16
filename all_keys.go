package goriak

import (
	"errors"
	riak "gopkg.in/zegl/goriak.v3/deps/riak-go-client"
)

type AllKeysCommand struct {
	c       *Command
	builder *riak.ListKeysCommandBuilder
}

// AllKeys returns all keys in the set bucket.
// The response will be sent in multiple batches to callback
func (c *Command) AllKeys(callback func([]string) error) *AllKeysCommand {
	builder := riak.NewListKeysCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithCallback(callback).
		WithAllowListing().
		WithStreaming(true)

	return &AllKeysCommand{
		c:       c,
		builder: builder,
	}
}

func (c *AllKeysCommand) Run(session *Session) (*Result, error) {
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
