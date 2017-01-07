package goriak

import (
	riak "github.com/basho/riak-go-client"
)

type requestData struct {
	bucket     string
	bucketType string
	key        string
}

// Get retreives a Map from Riak.
// Get performs automatic conversion from Riak Maps to your Go datatype.
// See Set() for more information.
func (c Command) Get(key string, output interface{}) Command {
	c.key = key
	c.output = output

	cmd, err := riak.NewFetchMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithKey(c.key).
		Build()

	if err != nil {
		c.err = err
		return c
	}

	c.riakCommand = cmd
	c.commandType = riakFetchMapCommand

	return c
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
func (c Command) Set(val interface{}) Command {

	riakContext, op, err := encodeInterface(val, requestData{
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
		WithMapOperation(filterMapOperation(&c, op, []string{}, nil))

	if len(riakContext) > 0 {
		builder.WithContext(riakContext)
	}

	c.updateMapCommandBuilder = builder
	c.commandType = riakUpdateMapCommand

	return c
}

// Takes a *riakMapOperation (our type) applies any filtering rules set on the Command
// Returns a *riak.MapOperation (from riak-go-client)
func filterMapOperation(cmd *Command, input *riakMapOperation, path []string, op *riak.MapOperation) *riak.MapOperation {

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
