package goriak

import (
	"encoding/json"
	"errors"
	riak "gopkg.in/zegl/goriak.v3/deps/riak-go-client"
)

type GetRawCommand struct {
	builder *riak.FetchValueCommandBuilder

	c *Command

	key string

	// Used in conflict resolution and middleware
	bucket     string
	bucketType string

	output      interface{}
	outputBytes *[]byte

	isRawOutput bool

	// VClock is used in conflict resolution
	// http://docs.basho.com/riak/kv/2.1.4/developing/usage/conflict-resolution/
	vclock               []byte
	conflictResolverFunc func([]ConflictObject) ResolvedConflict
}

func (c *GetRawCommand) ConflictResolver(fn func([]ConflictObject) ResolvedConflict) *GetRawCommand {
	c.conflictResolverFunc = fn
	return c
}

func (c *GetRawCommand) fetchValueWithResolver(session *Session, values []*riak.Object) ([]byte, []byte, error) {

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
			SetRaw(useObj.Value).
			Key(c.key).
			WithContext(useObj.VClock).
			Run(session)

		return useObj.Value, useObj.VClock, nil
	}

	return values[0].Value, values[0].VClock, nil
}

func (c *GetRawCommand) WithPr(pr uint32) *GetRawCommand {
	c.builder.WithPr(pr)
	return c
}

func (c *GetRawCommand) WithR(r uint32) *GetRawCommand {
	c.builder.WithR(r)
	return c
}

func runMiddleware(middlewarer RunMiddlewarer, middlewareList []RunMiddleware, execFunc func(*Session) (*Result, error), session *Session) (*Result, error) {
	// Keep track of whick middleware that we should execute next
	middlewareI := 0

	// Needed so that next can call itself
	var next2 func() (*Result, error)

	next := func() (*Result, error) {
		if middlewareI == len(middlewareList) {
			return execFunc(session)
		}

		middlewareI++

		return middlewareList[middlewareI-1](middlewarer, next2)
	}

	next2 = next

	return next()
}

func (c *GetRawCommand) Run(session *Session) (*Result, error) {
	middlewarer := &getRawMiddlewarer{
		cmd: c,
	}

	return runMiddleware(middlewarer, c.c.runMiddleware, c.runExec, session)
}

func (c *GetRawCommand) runExec(session *Session) (*Result, error) {
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	fetchCmd := cmd.(*riak.FetchValueCommand)

	if !fetchCmd.Success() {
		return nil, errors.New("Not successful")
	}

	if fetchCmd.Response.IsNotFound {
		return &Result{NotFound: true}, errors.New("Not found")
	}

	value, context, err := c.fetchValueWithResolver(session, fetchCmd.Response.Values)
	if err != nil {
		return nil, err
	}

	if c.isRawOutput {
		*c.outputBytes = value
	} else {
		err = json.Unmarshal(value, c.output)
		if err != nil {
			return nil, err
		}
	}

	return &Result{
		Key:     c.key,
		Context: context,
	}, nil
}
