package goriak

import (
	"errors"
	riak "gopkg.in/zegl/goriak.v3/deps/riak-go-client"
)

type UpdateHyperLogLogCommand struct {
	builder    *riak.UpdateHllCommandBuilder
	returnBody bool
	key        string
}

func (c *Command) UpdateHyperLogLog() *UpdateHyperLogLogCommand {
	b := riak.NewUpdateHllCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	return &UpdateHyperLogLogCommand{
		builder: b,
	}
}

func (c *UpdateHyperLogLogCommand) Add(val []byte) *UpdateHyperLogLogCommand {
	c.builder.WithAdditions(val)
	return c
}

func (c *UpdateHyperLogLogCommand) AddMultiple(vals ...[]byte) *UpdateHyperLogLogCommand {
	c.builder.WithAdditions(vals...)
	return c
}

func (c *UpdateHyperLogLogCommand) Key(key string) *UpdateHyperLogLogCommand {
	c.builder.WithKey(key)
	c.key = key
	return c
}

// WithPw sets the number of primary nodes  that must report back a successful write for the command to be successful.
func (c *UpdateHyperLogLogCommand) WithPw(pw uint32) *UpdateHyperLogLogCommand {
	c.builder.WithPw(pw)
	return c
}

// WithDw sets the number of nodes that must report back a successful write to their backend storage for the command to be successful.
func (c *UpdateHyperLogLogCommand) WithDw(dw uint32) *UpdateHyperLogLogCommand {
	c.builder.WithDw(dw)
	return c
}

// WithW sets the number of nodes that must report back a successful write for the command to be successful.
func (c *UpdateHyperLogLogCommand) WithW(w uint32) *UpdateHyperLogLogCommand {
	c.builder.WithW(w)
	return c
}

func (c *UpdateHyperLogLogCommand) ReturnBody(returnBody bool) *UpdateHyperLogLogCommand {
	c.builder.WithReturnBody(returnBody)
	c.returnBody = returnBody
	return c
}

// Run executes the command. If ReturnBody() is not set to true the result will be nil.
func (c *UpdateHyperLogLogCommand) Run(session *Session) (*HyperLogLogResult, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	res, ok := cmd.(*riak.UpdateHllCommand)
	if !ok {
		return nil, errors.New("Could not convert result")
	}

	if !res.Success() {
		return nil, errors.New("Execution not successful")
	}

	if !c.returnBody {
		return nil, nil
	}

	key := c.key
	if c.key == "" {
		key = res.Response.GeneratedKey
	}

	return &HyperLogLogResult{
		Key:         key,
		Cardinality: res.Response.Cardinality,
	}, nil
}
