package goriak

import (
	"errors"
	riak "gopkg.in/zegl/goriak.v3/deps/riak-go-client"
)

type SetRawCommand struct {
	c *Command

	// Riak builder type for SetValue
	// Other commands populate riakComand directly
	// SetJSON and SetRaw will populate these values instead
	storeValueCommandBuilder *riak.StoreValueCommandBuilder
	storeValueObject         *riak.Object

	key string

	err error
}

func (c *SetRawCommand) Key(key string) *SetRawCommand {
	c.storeValueCommandBuilder.WithKey(key)
	c.key = key
	return c
}

func (c *SetRawCommand) AddToIndex(key, value string) *SetRawCommand {
	c.storeValueObject.AddToIndex(key, value)
	return c
}

// Durable writes (to backend storage)
func (c *SetRawCommand) WithDw(val uint32) *SetRawCommand {
	c.storeValueCommandBuilder.WithDw(val)
	return c
}

// Primary node writes
func (c *SetRawCommand) WithPw(val uint32) *SetRawCommand {
	c.storeValueCommandBuilder.WithPw(val)
	return c
}

// Node writes
func (c *SetRawCommand) WithW(val uint32) *SetRawCommand {
	c.storeValueCommandBuilder.WithW(val)
	return c
}

func (c *SetRawCommand) WithContext(val []byte) *SetRawCommand {
	c.storeValueCommandBuilder.WithVClock(val)
	return c
}

// buildStoreValueCommand completes the building if the StoreValueCommand used by SetRaw and SetJSON
func (c *SetRawCommand) Run(session *Session) (*Result, error) {
	if c.err != nil {
		return nil, c.err
	}

	// Set object
	c.storeValueCommandBuilder.WithContent(c.storeValueObject)

	middlewarer := setRawMiddlewarer{
		cmd: c,
	}

	return runMiddleware(middlewarer, c.c.runMiddleware, c.riakExecute, session)
}

func (c *SetRawCommand) riakExecute(session *Session) (*Result, error) {

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

	if c.key == "" {
		c.key = storeCmd.Response.GeneratedKey
	}

	return &Result{
		Key: c.key,
	}, nil
}
