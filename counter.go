package goriak

import (
	"errors"

	riak "github.com/basho/riak-go-client"
)

// NewCounter returns a partial Counter
// Counters returned by NewCounter() can only be updated with SetMap().
// Counter.Exec() will not work on counters returned by NewCounter()
func NewCounter() *Counter {
	return &Counter{
		val:        0,
		increaseBy: 0,
	}
}

// Counter is a wapper to handle Riak Counters
// Counter needs to be initialized by GetMap() to fully function
type Counter struct {
	path []string    // Path to the counter (can be a map in a map in a map, etc..)
	name string      // Name of the counter
	key  requestData // bucket information

	val        int64
	increaseBy int64
}

// Increase the value in the Counter by i
// The value in Counter.Value() will be updated directly
// Increase() will not save the changes to Riak directly
func (c *Counter) Increase(i int64) *Counter {
	if c == nil {
		return nil
	}

	c.val += i
	c.increaseBy += i

	return c
}

// Value returns the value in the Counter
func (c *Counter) Value() int64 {
	return c.val
}

// Exec saves changes made to the Counter to Riak
// Exec only works on Counters initialized by GetMap()
func (c *Counter) Exec(client *Session) error {

	if c == nil {
		return errors.New("Nil Counter")
	}

	if c.name == "" {
		return errors.New("Unknown path to counter. Retrieve counter with GetMap before updating the counter")
	}

	op := &riak.MapOperation{}
	outerOp := op

	// Traverse c.path so that we increment the correct counter in nested maps
	for _, subMapName := range c.path {
		op = op.Map(subMapName)
	}

	op.IncrementCounter(c.name, c.increaseBy)

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.key.bucket).
		WithBucketType(c.key.bucketType).
		WithKey(c.key.key).
		WithMapOperation(outerOp).
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
