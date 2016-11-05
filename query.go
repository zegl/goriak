package goriak

import (
	"encoding/json"
	"errors"

	riak "github.com/basho/riak-go-client"
)

type riakCommandType uint8

const (
	riakUnknownCommand riakCommandType = iota
	riakFetchMapCommand
	riakUpdateMapCommand
	riakStoreValueCommand
	riakFetchValueCommandJSON
	riakFetchValueCommandRaw
	riakListKeysCommand
	riakDeleteValueCommand
	riakSecondaryIndexQueryCommand
)

// Command is the main query builder object
type Command struct {
	// Key information
	bucket     string
	bucketType string
	key        string

	// Limit result for SecondaryIndex queries
	limit uint32

	// Temporary information used by Run()
	err         error
	riakCommand riak.Command
	commandType riakCommandType

	// Output variables
	output      interface{}
	outputBytes *[]byte

	// Indexes used by SetJSON() and SetRaw()
	indexes map[string][]string

	// Riak builder type for SetValue
	// Other commands populate riakComand directly
	// SetJSON and SetRaw will populate these values instead
	storeValueCommandBuilder *riak.StoreValueCommandBuilder
	storeValueObject         *riak.Object

	// Riak builder type for KeysInIndex
	secondaryIndexQueryCommandBuilder *riak.SecondaryIndexQueryCommandBuilder

	// Riak builder type for MapOperation
	updateMapCommandBuilder *riak.UpdateMapCommandBuilder
}

// Result contains your query result data from Run()
type Result struct {
	NotFound bool   // Wether or not the item was not found when using Get, GetJSON, or GetRaw.
	Key      string // Returns your automatically generated key when using Set, SetJSON, or SetRaw.
	Context  []byte // Returns the Riak Context used in map operations. Is set when using Get.
}

// Bucket specifies the bucket and bucket type that your following command will be performed on.
func Bucket(bucket, bucketType string) Command {
	return Command{
		bucket:     bucket,
		bucketType: bucketType,
	}
}

// Key specifies the Riak key that following commands such as Get() and MapOperation()
func (c Command) Key(key string) Command {
	c.key = key
	return c
}

// Limit sets the limit returned in KeysInIndex
// A limit of 0 means unlimited
func (c Command) Limit(limit uint32) Command {
	c.limit = limit
	return c
}

// Run performs the action built in Command and runs it against the Riak connection specified by Session.
func (c Command) Run(session *Session) (*Result, error) {

	if session == nil {
		return nil, errors.New("No session provided")
	}

	// Commands that hasn't been built yet
	switch c.commandType {
	case riakStoreValueCommand:
		c = c.buildStoreValueCommand()
	case riakSecondaryIndexQueryCommand:
		c = c.buildSecondaryIndexQueryCommand()
	case riakUpdateMapCommand:
		c = c.buildUpdateMapQueryCommand()
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

	case riakStoreValueCommand:
		cmd := c.riakCommand.(*riak.StoreValueCommand)
		return c.resultStoreValueCommand(cmd)

	case riakFetchValueCommandJSON:
		cmd := c.riakCommand.(*riak.FetchValueCommand)
		return c.resultFetchValueCommandJSON(cmd)

	case riakFetchValueCommandRaw:
		cmd := c.riakCommand.(*riak.FetchValueCommand)
		return c.resultFetchValueCommandRaw(cmd)

	case riakListKeysCommand:
		cmd := c.riakCommand.(*riak.ListKeysCommand)
		return c.resultListKeysCommand(cmd)

	case riakDeleteValueCommand:
		cmd := c.riakCommand.(*riak.DeleteValueCommand)
		return c.resultDeleteValueCommand(cmd)

	case riakSecondaryIndexQueryCommand:
		cmd := c.riakCommand.(*riak.SecondaryIndexQueryCommand)
		return c.resultSecondaryIndexQueryCommand(cmd)

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
		Key:     c.key,
		Context: cmd.Response.Context,
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

func (c Command) resultStoreValueCommand(cmd *riak.StoreValueCommand) (*Result, error) {
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

func (c Command) resultFetchValueCommandJSON(cmd *riak.FetchValueCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	if cmd.Response.IsNotFound {
		return &Result{
			NotFound: true,
		}, errors.New("Not found")
	}

	err := json.Unmarshal(cmd.Response.Values[0].Value, c.output)

	if err != nil {
		return nil, err
	}

	return &Result{
		Key: c.key,
	}, nil
}

func (c Command) resultFetchValueCommandRaw(cmd *riak.FetchValueCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	if cmd.Response.IsNotFound {
		return &Result{
			NotFound: true,
		}, errors.New("Not found")
	}

	*c.outputBytes = cmd.Response.Values[0].Value

	return &Result{
		Key: c.key,
	}, nil
}

func (c Command) resultListKeysCommand(cmd *riak.ListKeysCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}

func (c Command) resultDeleteValueCommand(cmd *riak.DeleteValueCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}

func (c Command) resultSecondaryIndexQueryCommand(cmd *riak.SecondaryIndexQueryCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}

// buildStoreValueCommand completes the building if the StoreValueCommand used by SetRaw and SetJSON
func (c Command) buildStoreValueCommand() Command {
	// Set key
	if c.key != "" {
		c.storeValueCommandBuilder.WithKey(c.key)
	}

	// Add indexes to object if needed
	// Indexes from Command.AddToIndex()
	for indexName, values := range c.indexes {
		for _, val := range values {
			c.storeValueObject.AddToIndex(indexName, val)
		}
	}

	// Set object
	c.storeValueCommandBuilder.WithContent(c.storeValueObject)

	// Build it!
	c.riakCommand, c.err = c.storeValueCommandBuilder.Build()
	return c
}

// buildSecondaryIndexQueryCommand completes the buildinf of the SecondaryIndexQueryCommand used by KeysInIndex
func (c Command) buildSecondaryIndexQueryCommand() Command {
	// Set limit
	if c.limit != 0 {
		c.secondaryIndexQueryCommandBuilder.WithMaxResults(c.limit)
	}

	// Build it!
	c.riakCommand, c.err = c.secondaryIndexQueryCommandBuilder.Build()
	return c
}

func (c Command) buildUpdateMapQueryCommand() Command {
	if c.key != "" {
		c.updateMapCommandBuilder.WithKey(c.key)
	}

	// Build it!
	c.riakCommand, c.err = c.updateMapCommandBuilder.Build()
	return c
}
