package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

func NewRegister() *Register {
	return &Register{}
}

type Register struct {
	helper

	val []byte
}

func (r *Register) Value() []byte {
	return r.val
}

func (r *Register) String() string {
	return string(r.val)
}

func (r *Register) Set(val []byte) *Register {
	r.val = val
	return r
}

func (r *Register) SetString(val string) *Register {
	r.val = []byte(val)
	return r
}

func (r *Register) Exec(client *Session) error {
	if r == nil {
		return errors.New("Nil Register")
	}

	if r.name == "" {
		return errors.New("Unknown path to Register. Retrieve Register with Get or Set before updating the Register")
	}

	// Validate s.key
	if r.key.bucket == "" || r.key.bucketType == "" || r.key.key == "" {
		return errors.New("Invalid key in Register Exec()")
	}

	op := &riak.MapOperation{}
	outerOp := op

	// Traverse c.path so that we increment the correct counter in nested maps
	for _, subMapName := range r.path {
		op = op.Map(subMapName)
	}

	op.SetRegister(r.name, r.val)

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(r.key.bucket).
		WithBucketType(r.key.bucketType).
		WithKey(r.key.key).
		WithMapOperation(outerOp).
		WithContext(r.context).
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
