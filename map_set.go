package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

type requestData struct {
	bucket     string
	bucketType string
	key        string
}

type commandMapSet struct {
	*Command

	key   string
	input interface{}

	builder *riak.UpdateMapCommandBuilder

	includeFilter [][]string
	excludeFilter [][]string
}

/*
Set automatically converts your Go datatype to the equivalent type in Riak

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
	| time.Time  | register  |
*/
func (cmd *Command) Set(val interface{}) *commandMapSet {

	c := &commandMapSet{
		Command: cmd,
		input:   val,
	}

	c.builder = riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	/*riakContext, op, err := encodeInterface(val, requestData{
		bucket:     c.bucket,
		bucketType: c.bucketType,
		key:        c.key,
	})

	if err != nil {
		c.err = err
		return c
	}

	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithMapOperation(filterMapOperation(c, op, []string{}, nil))

	if len(riakContext) > 0 {
		builder.WithContext(riakContext)
	}

	c.updateMapCommandBuilder = builder
	c.commandType = riakUpdateMapCommand*/

	return c
}

func (c *commandMapSet) Key(key string) *commandMapSet {
	c.key = key
	return c
}

// Takes a *riakMapOperation (our type) applies any filtering rules set on the Command
// Returns a *riak.MapOperation (from riak-go-client)
func filterMapOperation(cmd *commandMapSet, input *riakMapOperation, path []string, op *riak.MapOperation) *riak.MapOperation {

	if op == nil {
		op = &riak.MapOperation{}
	}

	// Not implemented:
	// RemoveRegister()
	// RemoveCounter()
	// RemoveFlag()
	// RemoveMap()
	// RemoveSet()

	// AddToSet
	for key, values := range input.addToSets {
		for _, value := range values {
			if cmd.filterAllowPath(append(path, key)...) {
				op.AddToSet(key, value)
			}
		}
	}

	// RemoveFromSet
	for key, values := range input.removeFromSets {
		for _, value := range values {
			if cmd.filterAllowPath(append(path, key)...) {
				op.RemoveFromSet(key, value)
			}
		}
	}

	// SetRegister
	for key, value := range input.registersToSet {
		if cmd.filterAllowPath(append(path, key)...) {
			op.SetRegister(key, value)
		}
	}

	// Map
	for key, value := range input.maps {
		// No filtering is performed here
		subOp := op.Map(key)
		filterMapOperation(cmd, value, append(path, key), subOp)
	}

	// IncrementCounter
	for key, value := range input.incrementCounters {
		if cmd.filterAllowPath(append(path, key)...) {
			op.IncrementCounter(key, value)
		}
	}

	// SetFlag
	for key, value := range input.flagsToSet {
		if cmd.filterAllowPath(append(path, key)...) {
			op.SetFlag(key, value)
		}
	}

	return op
}

func (c *commandMapSet) Run(session *Session) (*Result, error) {
	riakContext, op, err := encodeInterface(c.input, requestData{
		bucket:     c.bucket,
		bucketType: c.bucketType,
		key:        c.key,
	})
	if err != nil {
		return nil, err
	}

	// Set context
	if len(riakContext) > 0 {
		c.builder.WithContext(riakContext)
	}

	// Set the map operation
	c.builder.WithMapOperation(filterMapOperation(c, op, []string{}, nil))

	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	updateCmd := cmd.(*riak.UpdateMapCommand)

	if !updateCmd.Success() {
		return nil, errors.New("Not successful")
	}

	if c.key != "" {
		return &Result{
			Key: c.key,
		}, nil
	}

	return &Result{
		Key: updateCmd.Response.GeneratedKey,
	}, nil
}
