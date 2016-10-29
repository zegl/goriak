package goriak

import (
	"errors"

	riak "github.com/basho/riak-go-client"
)

func NewCounter() *Counter {
	return &Counter{
		val:        0,
		increaseBy: 0,
	}
}

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

	if c.name == "" {
		return errors.New("Unknown path to counter. Retreive counter with GetMap before updating the counter")
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
