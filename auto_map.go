package goriak

import (
	riak "github.com/basho/riak-go-client"
)

type requestData struct {
	bucket     string
	bucketType string
	key        string
}

// Get retreives a Map from Riak.
// Get performs automatic conversion from Riak Maps to your Go datatype.
// See Set() for more information.
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

/*
Set automatically converts your Go datatype to the equivalent type in Riak

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
func (c Command) Set(val interface{}) Command {

	riakContext, op, err := encodeInterface(val, requestData{
		bucket:     c.bucket,
		bucketType: c.bucketType,
	})

	if err != nil {
		c.err = err
		return c
	}

	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithMapOperation(op)

	if len(riakContext) > 0 {
		builder.WithContext(riakContext)
	}

	c.updateMapCommandBuilder = builder
	c.commandType = riakUpdateMapCommand

	return c
}
