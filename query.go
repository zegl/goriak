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

	// VClock is used in conflict resolution
	// http://docs.basho.com/riak/kv/2.1.4/developing/usage/conflict-resolution/
	vclock               []byte
	conflictResolverFunc func([]ConflictObject) ResolvedConflict

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

	// Riak builder type for GetRaw() and GetJSON()
	fetchValueCommandBuilder *riak.FetchValueCommandBuilder

	// Riak builder type for Delete()
	deleteValueCommandBuilder *riak.DeleteValueCommandBuilder

	// Riak Consistency options
	riakPW uint32 // Primary nodes during write
	riakDW uint32 // Nodes that successfully can write to backend storage
	riakW  uint32 // Nodes during write
	riakRW uint32 // Nodes that successfully deleted item from backend storage
	riakPR uint32 // Primary nodes during read
	riakR  uint32 // Nodes during read
}

// Result contains your query result data from Run()
type Result struct {
	NotFound bool   // Wether or not the item was not found when using Get, GetJSON, or GetRaw.
	Key      string // Returns your automatically generated key when using Set, SetJSON, or SetRaw.
	Context  []byte // Returns the Riak Context used in map operations. Is set when using Get.
	VClock   []byte
}

// Bucket specifies the bucket and bucket type that your following command will be performed on.
func Bucket(bucket, bucketType string) *Command {
	return &Command{
		bucket:     bucket,
		bucketType: bucketType,
	}
}

// Key specifies the Riak key that following commands such as Get() and MapOperation()
func (c *Command) Key(key string) *Command {
	c.key = key
	return c
}

func (c *Command) VClock(vclock []byte) *Command {
	c.vclock = vclock
	return c
}

func (c *Command) ConflictResolver(fn func([]ConflictObject) ResolvedConflict) *Command {
	c.conflictResolverFunc = fn
	return c
}

// Limit sets the limit returned in KeysInIndex
// A limit of 0 means unlimited
func (c *Command) Limit(limit uint32) *Command {
	c.limit = limit
	return c
}

// Run performs the action built in Command and runs it against the Riak connection specified by Session.
func (c *Command) Run(session *Session) (*Result, error) {

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
	case riakFetchValueCommandJSON:
		fallthrough
	case riakFetchValueCommandRaw:
		c = c.buildFetchValueCommand()
	case riakDeleteValueCommand:
		c = c.buildDeleteValueCommand()
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
		return c.resultFetchValueCommandJSON(session, cmd)

	case riakFetchValueCommandRaw:
		cmd := c.riakCommand.(*riak.FetchValueCommand)
		return c.resultFetchValueCommandRaw(session, cmd)

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

func (c *Command) resultFetchMapCommand(cmd *riak.FetchMapCommand) (*Result, error) {
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

func (c *Command) resultUpdateMapCommand(cmd *riak.UpdateMapCommand) (*Result, error) {
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

func (c *Command) resultStoreValueCommand(cmd *riak.StoreValueCommand) (*Result, error) {
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

func (c *Command) fetchValueWithResolver(session *Session, values []*riak.Object) ([]byte, []byte, error) {

	// Conflict resolution necessary
	if len(values) > 1 {

		// No explicit resolver func
		if c.conflictResolverFunc == nil {

			// Use conflict resolver func from interface
			if resolver, ok := c.output.(ConflictResolver); ok {
				c.conflictResolverFunc = resolver.ConflictResolver
			} else {
				return []byte{}, []byte{}, errors.New("goriak: Had conflict, but no conflict resolver")
			}
		}

		objs := make([]ConflictObject, len(values))

		for i, v := range values {
			objs[i] = ConflictObject{
				Value:        v.Value,
				LastModified: v.LastModified,
				VClock:       v.VClock,
			}
		}

		useObj := c.conflictResolverFunc(objs)

		if len(useObj.VClock) == 0 {
			return []byte{}, []byte{}, errors.New("goriak: Invalid value from conflict resolver")
		}

		// Save resolution
		Bucket(c.bucket, c.bucketType).
			Key(c.key).
			VClock(useObj.VClock).
			SetRaw(useObj.Value).
			Run(session)

		return useObj.Value, useObj.VClock, nil
	}

	return values[0].Value, values[0].VClock, nil
}

func (c *Command) resultFetchValueCommandJSON(session *Session, cmd *riak.FetchValueCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	if cmd.Response.IsNotFound {
		return &Result{
			NotFound: true,
		}, errors.New("Not found")
	}

	value, vclock, err := c.fetchValueWithResolver(session, cmd.Response.Values)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(value, c.output)

	if err != nil {
		return nil, err
	}

	return &Result{
		Key:    c.key,
		VClock: vclock,
	}, nil
}

func (c *Command) resultFetchValueCommandRaw(session *Session, cmd *riak.FetchValueCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	if cmd.Response.IsNotFound {
		return &Result{
			NotFound: true,
		}, errors.New("Not found")
	}

	value, vclock, err := c.fetchValueWithResolver(session, cmd.Response.Values)

	if err != nil {
		return nil, err
	}

	*c.outputBytes = value

	return &Result{
		Key:    c.key,
		VClock: vclock,
	}, nil
}

func (c *Command) resultListKeysCommand(cmd *riak.ListKeysCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}

func (c *Command) resultDeleteValueCommand(cmd *riak.DeleteValueCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}

func (c *Command) resultSecondaryIndexQueryCommand(cmd *riak.SecondaryIndexQueryCommand) (*Result, error) {
	if !cmd.Success() {
		return nil, errors.New("Not successful")
	}

	return &Result{}, nil
}
