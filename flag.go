package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

func NewFlag() *Flag {
	return &Flag{}
}

type Flag struct {
	helper

	val bool
}

func (f *Flag) Value() bool {
	return f.val
}

func (f *Flag) Set(val bool) *Flag {
	f.val = val
	return f
}

func (f *Flag) Exec(client *Session) error {
	if f == nil {
		return errors.New("Nil Flag")
	}

	if f.name == "" {
		return errors.New("Unknown path to Flag. Retrieve Flag with Get or Set before updating the Flag")
	}

	// Validate s.key
	if f.key.bucket == "" || f.key.bucketType == "" || f.key.key == "" {
		return errors.New("Invalid key in Flag Exec()")
	}

	op := &riak.MapOperation{}
	outerOp := op

	// Traverse c.path so that we increment the correct counter in nested maps
	for _, subMapName := range f.path {
		op = op.Map(subMapName)
	}

	op.SetFlag(f.name, f.val)

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(f.key.bucket).
		WithBucketType(f.key.bucketType).
		WithKey(f.key.key).
		WithMapOperation(outerOp).
		WithContext(f.context).
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
