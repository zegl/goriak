package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type MapGetCommand struct {
	c       *Command
	output  interface{}
	key     string
	builder *riak.FetchMapCommandBuilder
}

// Get retreives a Map from Riak.
// Get performs automatic conversion from Riak Maps to your Go datatype.
// See Set() for more information.
func (c *Command) Get(key string, output interface{}) *MapGetCommand {
	builder := riak.NewFetchMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(key)

	return &MapGetCommand{
		c:       c,
		output:  output,
		key:     key,
		builder: builder,
	}
}

func (c *MapGetCommand) Run(session *Session) (*Result, error) {
	middlewarer := &getMiddlewarer{
		cmd: c,
	}

	return runMiddleware(middlewarer, c.c.runMiddleware, c.riakExec, session)
}

func (c *MapGetCommand) riakExec(session *Session) (*Result, error) {
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
		bucket:     c.c.bucket,
		bucketType: c.c.bucketType,
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

type getMiddlewarer struct {
	cmd *MapGetCommand
}

func (c *getMiddlewarer) Key() string {
	return c.cmd.key
}

func (c *getMiddlewarer) Bucket() string {
	return c.cmd.c.bucket
}

func (c *getMiddlewarer) BucketType() string {
	return c.cmd.c.bucketType
}

func (c getMiddlewarer) Command() CommandType {
	return CommandTypeGet
}
