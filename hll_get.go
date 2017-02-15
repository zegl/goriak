package goriak

import (
	riak "github.com/basho/riak-go-client"

	"errors"
)

type FetchHyperLogLogCommand struct {
	builder *riak.FetchHllCommandBuilder
	key     string
}

type HyperLogLogResult struct {
	Key         string
	NotFound    bool
	Cardinality uint64
}

func (c *Command) GetHyperLogLog(key string) *FetchHyperLogLogCommand {
	b := riak.NewFetchHllCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	return &FetchHyperLogLogCommand{
		builder: b,
		key:     key,
	}
}

func (c *FetchHyperLogLogCommand) WithPr(pr uint32) *FetchHyperLogLogCommand {
	c.builder.WithPr(pr)
	return c

}
func (c *FetchHyperLogLogCommand) WithR(r uint32) *FetchHyperLogLogCommand {
	c.builder.WithPr(r)
	return c
}

func (c *FetchHyperLogLogCommand) Run(session *Session) (*HyperLogLogResult, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	res, ok := cmd.(*riak.FetchHllCommand)
	if !ok {
		return nil, errors.New("Could not convert result")
	}

	if !res.Success() {
		return nil, errors.New("Execution not successful")
	}

	return &HyperLogLogResult{
		NotFound:    res.Response.IsNotFound,
		Cardinality: res.Response.Cardinality,
		Key:         c.key,
	}, nil
}
