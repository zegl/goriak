package goriak

import (
	"errors"

	riak "github.com/basho/riak-go-client"
)

type riakCommandType uint8

const (
	riakUnknownCommand riakCommandType = iota
	riakFetchMapCommand
	riakUpdateMapCommand
)

type Command struct {
	bucket     string
	bucketType string
	key        string

	err         error
	riakCommand riak.Command
	commandType riakCommandType

	output interface{}
}

type Result struct {
	NotFound bool
	Key      string
}

func Bucket(bucket, bucketType string) Command {
	return Command{
		bucket:     bucket,
		bucketType: bucketType,
	}
}

func (c Command) Get(key string, output interface{}) Command {
	c.key = key
	c.output = output

	cmd, err := riak.NewFetchMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(c.key).
		Build()

	if err != nil {
		c.err = err
		return c
	}

	c.riakCommand = cmd
	c.commandType = riakFetchMapCommand

	return c
}

func (c Command) Key(key string) Command {
	c.key = key
	return c
}

/*
Insert automatically converts your Go datatype to the equivalent type in Riak

	|  Go Type   | Riak Type |
	|------------|-----------|
	| struct     | map       |
	| string     | register  |
	| [n]byte    | register  |
	| []byte     | register  |
	| []slice    | set       |
	| []slice    | set       |
	| [][]byte   | set       |
	| map        | map       |
*/
func (c Command) Insert(val interface{}) Command {
	riakContext, op, err := encodeInterface(val)

	if err != nil {
		c.err = err
		return c
	}

	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithMapOperation(op)

	if c.key != "" {
		builder.WithKey(c.key)
	}

	if len(riakContext) > 0 {
		builder.WithContext(riakContext)
	}

	cmd, err := builder.Build()

	if err != nil {
		c.err = err
		return c
	}

	c.riakCommand = cmd
	c.commandType = riakUpdateMapCommand

	return c
}

func (c Command) Run(session *Session) (*Result, error) {

	if session == nil {
		return nil, errors.New("No session provided")
	}

	// Error from previous steps
	if c.err != nil {
		return nil, c.err
	}

	if c.riakCommand == nil {
		return nil, errors.New("No command?")
	}

	if session.riak == nil {
		return nil, errors.New("Not connected to Riak")
	}

	err := session.riak.Execute(c.riakCommand)

	if err != nil {
		return nil, err
	}

	switch c.commandType {
	case riakFetchMapCommand:
		cmd := c.riakCommand.(*riak.FetchMapCommand)
		return c.resultFetchMapCommand(cmd)

	case riakUpdateMapCommand:
		cmd := c.riakCommand.(*riak.UpdateMapCommand)
		return c.resultUpdateMapCommand(cmd)

	default:
		return nil, errors.New("Unknown response?")
	}
}

func (c Command) resultFetchMapCommand(cmd *riak.FetchMapCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	if cmd.Response.IsNotFound {
		return &Result{
			NotFound: true,
		}, errors.New("Not found")
	}

	req := requestData{
		bucket:     c.bucket,
		bucketType: c.bucketType,
		key:        c.key,
	}

	err := decodeInterface(cmd.Response, c.output, req)

	if err != nil {
		return nil, err
	}

	return &Result{
		Key: c.key,
	}, nil
}

func (c Command) resultUpdateMapCommand(cmd *riak.UpdateMapCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	if c.key != "" {
		return &Result{
			Key: c.key,
		}, nil
	}

	return &Result{
		Key: cmd.Response.GeneratedKey,
	}, nil
}
