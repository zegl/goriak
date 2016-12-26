package goriak

import (
	"encoding/json"
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
	helper

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
// If the commad succeeds the counter will be updated with the value in the response from Riak
func (c *Counter) Exec(client *Session) error {

	if c == nil {
		return errors.New("Nil Counter")
	}

	if c.name == "" {
		return errors.New("Unknown path to Counter. Retrieve Counter with Get or Set before updating the Counter")
	}

	// Validate c.key
	if c.key.bucket == "" || c.key.bucketType == "" || c.key.key == "" {
		return errors.New("Invalid key in Counter Exec()")
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
		WithReturnBody(true).
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

	// Update c.val from the response
	m := res.Response.Map

	for _, subMapName := range c.path {
		if _, ok := m.Maps[subMapName]; ok {
			m = m.Maps[subMapName]
		}
	}

	if resVal, ok := m.Counters[c.name]; ok {
		c.val = resVal
	}

	// Reset increase counter
	c.increaseBy = 0

	return nil
}

// MarshalJSON satisfies the JSON interface
func (c Counter) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.val)
}

// UnmarshalJSON satisfies the JSON interface
func (c *Counter) UnmarshalJSON(data []byte) error {
	var value int64

	err := json.Unmarshal(data, &value)

	if err != nil {
		return err
	}

	c.val = value
	return nil
}
