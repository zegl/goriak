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

type MapSetCommand struct {
	c *Command

	bucket     string
	bucketType string
	key        string
	input      interface{}

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
func (c *Command) Set(val interface{}) *MapSetCommand {
	builder := riak.NewUpdateMapCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType)

	return &MapSetCommand{
		c:          c,
		input:      val,
		bucket:     c.bucket,
		bucketType: c.bucketType,
		builder:    builder,
	}
}

func (c *MapSetCommand) Key(key string) *MapSetCommand {
	c.key = key
	c.builder.WithKey(key)
	return c
}

// Takes a *riakMapOperation (our type) applies any filtering rules set on the Command
// Returns a *riak.MapOperation (from riak-go-client)
func filterMapOperation(cmd *MapSetCommand, input *riakMapOperation, path []string, op *riak.MapOperation) *riak.MapOperation {

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

func (c *MapSetCommand) WithPw(pw uint32) *MapSetCommand {
	c.builder.WithPw(pw)
	return c
}

func (c *MapSetCommand) WithDw(dw uint32) *MapSetCommand {
	c.builder.WithDw(dw)
	return c
}

func (c *MapSetCommand) WithW(w uint32) *MapSetCommand {
	c.builder.WithPw(w)
	return c
}

func (c *MapSetCommand) Run(session *Session) (*Result, error) {
	middlewarer := &setMiddlewarer{
		cmd: c,
	}

	return runMiddleware(middlewarer, c.c.runMiddleware, c.riakExec, session)
}

func (c *MapSetCommand) riakExec(session *Session) (*Result, error) {
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
	op2 := filterMapOperation(c, op, []string{}, nil)

	c.builder.WithMapOperation(op2)

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

type setMiddlewarer struct {
	cmd *MapSetCommand
}

func (c *setMiddlewarer) Key() string {
	return c.cmd.key
}

func (c *setMiddlewarer) Bucket() string {
	return c.cmd.bucket
}

func (c *setMiddlewarer) BucketType() string {
	return c.cmd.bucketType
}
