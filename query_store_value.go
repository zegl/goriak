package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type commandSet struct {
	*Command

	// Riak builder type for SetValue
	// Other commands populate riakComand directly
	// SetJSON and SetRaw will populate these values instead
	storeValueCommandBuilder *riak.StoreValueCommandBuilder
	storeValueObject         *riak.Object

	key string

	err error
}

func (c *commandSet) Key(key string) *commandSet {
	c.storeValueCommandBuilder.WithKey(key)
	c.key = key
	return c
}

func (c *commandSet) AddToIndex(key, value string) *commandSet {
	c.storeValueObject.AddToIndex(key, value)
	return c
}

// Durable writes (to backend storage)
func (c *commandSet) WithDw(val uint32) *commandSet {
	c.storeValueCommandBuilder.WithDw(val)
	return c
}

// Primary node writes
func (c *commandSet) WithPw(val uint32) *commandSet {
	c.storeValueCommandBuilder.WithPw(val)
	return c
}

// Node writes
func (c *commandSet) WithW(val uint32) *commandSet {
	c.storeValueCommandBuilder.WithW(val)
	return c
}

func (c *commandSet) VClock(val []byte) *commandSet {
	c.storeValueCommandBuilder.WithVClock(val)
	return c
}

// buildStoreValueCommand completes the building if the StoreValueCommand used by SetRaw and SetJSON
func (c *commandSet) Run(session *Session) (*Result, error) {
	if c.err != nil {
		return nil, c.err
	}

	// Set object
	c.storeValueCommandBuilder.WithContent(c.storeValueObject)

	// Build it!
	cmd, err := c.storeValueCommandBuilder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	storeCmd := cmd.(*riak.StoreValueCommand)

	if !storeCmd.Success() {
		return nil, errors.New("Not successful")
	}

	var key string

	if c.key != "" {
		key = c.key
	} else {
		key = storeCmd.Response.GeneratedKey
	}

	return &Result{
		Key: key,
	}, nil
}
