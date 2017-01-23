package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type commandMapGet struct {
	*Command

	key    string
	output interface{}

	builder *riak.FetchMapCommandBuilder
}

// Get retreives a Map from Riak.
// Get performs automatic conversion from Riak Maps to your Go datatype.
// See Set() for more information.
func (cmd *Command) Get(key string, output interface{}) *commandMapGet {

	c := &commandMapGet{
		Command: cmd,
	}

	c.key = key
	c.output = output

	c.builder = riak.NewFetchMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(c.key)

	return c
}

func (c *commandMapGet) Run(session *Session) (*Result, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	mapCommand := cmd.(*riak.FetchMapCommand)

	if !mapCommand.Success() {
		return nil, errors.New("Not successful")
	}

	if mapCommand.Response.IsNotFound {
		return &Result{
			NotFound: true,
		}, errors.New("Not found")
	}

	req := requestData{
		bucket:     c.bucket,
		bucketType: c.bucketType,
		key:        c.key,
	}

	err = decodeInterface(mapCommand.Response, c.output, req)
	if err != nil {
		return nil, err
	}

	return &Result{
		Key:     c.key,
		Context: mapCommand.Response.Context,
	}, nil
}
