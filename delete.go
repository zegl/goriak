package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type DeleteCommand struct {
	c       *Command
	builder *riak.DeleteValueCommandBuilder
}

// Delete deletes the value stored as key
func (c *Command) Delete(key string) *DeleteCommand {
	builder := riak.NewDeleteValueCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	return &DeleteCommand{
		c:       c,
		builder: builder,
	}
}

func (c *DeleteCommand) WithDw(dw uint32) *DeleteCommand {
	c.builder.WithDw(dw)
	return c
}

func (c *DeleteCommand) WithPw(pw uint32) *DeleteCommand {
	c.builder.WithPw(pw)
	return c
}

func (c *DeleteCommand) WithPr(pr uint32) *DeleteCommand {
	c.builder.WithPr(pr)
	return c
}

func (c *DeleteCommand) WithR(r uint32) *DeleteCommand {
	c.builder.WithR(r)
	return c
}

func (c *DeleteCommand) WithW(w uint32) *DeleteCommand {
	c.builder.WithW(w)
	return c
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
