package goriak

import (
//"errors"
//"reflect"

//riak "github.com/basho/riak-go-client"
)

type requestData struct {
	bucket     string
	bucketType string
	key        string
}

/*
SetMap automatically converts your Go datatype to the equivalent type in Riak

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
/*func (c *Client) SetMap(bucket, bucketType, key string, input interface{}) error {
	riakContext, op, err := encodeInterface(input)

	if err != nil {
		return err
	}

	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(bucket).
		WithKey(key).
		WithBucketType(bucketType).
		WithMapOperation(op)

	if len(riakContext) > 0 {
		builder.WithContext(riakContext)
	}

	cmd, err := builder.Build()

	if err != nil {
		return err
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err
	}

	return nil
}

// GetMap fetches data from Riak and decodes the result into your Go datatype.
// See SetMap for more info.
func (c *Client) GetMap(bucket, bucketType, key string, output interface{}) (err error, isNotFound bool) {

	// Type check
	if reflect.ValueOf(output).Kind() != reflect.Ptr {
		return errors.New("output needs to be a pointer"), false
	}

	cmd, err := riak.NewFetchMapCommandBuilder().
		WithBucket(bucket).
		WithKey(key).
		WithBucketType(bucketType).
		Build()

	if err != nil {
		return err, false
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err, false
	}

	ma := cmd.(*riak.FetchMapCommand)

	if !ma.Success() {
		return errors.New("Not successful"), false
	}

	if ma.Response.IsNotFound {
		return errors.New("Not found"), true
	}

	req := requestData{
		bucket:     bucket,
		bucketType: bucketType,
		key:        key,
	}

	err = decodeInterface(ma.Response, output, req)

	if err != nil {
		return err, false
	}

	return nil, false
}

func (c *Client) MapOperation(bucket, bucketType, key string, op riak.MapOperation, context []byte) error {
	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithKey(key).
		WithMapOperation(&op).
		WithContext(context).
		Build()

	if err != nil {
		return err
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err
	}

	res, ok := cmd.(*riak.UpdateMapCommand)

	if !ok {
		return errors.New("Could not convert")
	}

	if !res.Success() {
		return errors.New("Not successful")
	}

	return nil
}

// NewMapOperation returns a new riak.MapOperation that you can for advanced Riak operations
func NewMapOperation() riak.MapOperation {
	return riak.MapOperation{}
}
*/
