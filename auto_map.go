package goriak

import (
	"errors"
	"reflect"

	riak "github.com/basho/riak-go-client"
)

type requestData struct {
	bucket     string
	bucketType string
	key        string
}

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

type Counter struct {
	path []string    // Path to the counter (can be a map in a map in a map, etc..)
	name string      // Name of the counter
	key  requestData // bucket information

	val        int64
	increaseBy int64
}

func (c *Counter) Increase(i int64) *Counter {
	if c == nil {
		return nil
	}

	c.val += i
	c.increaseBy += i

	return c
}

func (c *Counter) Value() int64 {
	return c.val
}

func (c *Counter) Exec(client *Client) error {

	if c == nil {
		return errors.New("Nil Counter")
	}

	op := riak.MapOperation{}
	op.IncrementCounter(c.name, c.increaseBy)

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.key.bucket).
		WithBucketType(c.key.bucketType).
		WithKey(c.key.key).
		WithMapOperation(&op).
		Build()

	if err != nil {
		return err
	}

	err = client.riak.Execute(cmd)

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
