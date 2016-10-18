package goriak

import (
	"errors"
	"reflect"

	riak "github.com/basho/riak-go-client"
)

func (c *Client) SetMap(bucket, bucketType, key string, input interface{}) error {
	op, err := encodeInterface(input)

	if err != nil {
		return err
	}

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(bucket).
		WithKey(key).
		WithBucketType(bucketType).
		WithMapOperation(op).
		Build()

	if err != nil {
		return err
	}

	err = c.riak.Execute(cmd)

	if err != nil {
		return err
	}

	return nil
}

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

	err = decodeInterface(ma.Response, output)

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

func NewMapOperation() riak.MapOperation {
	return riak.MapOperation{}
}

/*type MapOperation struct {
	op *riak.MapOperation
}



func (mo *MapOperation) SetRegister(key name, value interface{}) {
	mo.op.SetRegister(key, )
}
*/
