package goriak

import (
	"bytes"
	"errors"

	riak "github.com/basho/riak-go-client"
)

func NewSet() *Set {
	return &Set{}
}

type Set struct {
	path    []string    // Path to the counter (can be a map in a map in a map, etc..)
	name    string      // Name of the counter
	key     requestData // bucket information
	context []byte      // riak context

	value   [][]byte // The full content
	adds    [][]byte // Not-yet performed Add actions (performed locally but not to Riak)
	removes [][]byte // Same as adds, but for removal of items
}

func (s *Set) Value() [][]byte {
	r := make([][]byte, 0)

	// Remove empty items
	for _, v := range s.value {
		if len(v) != 0 {
			r = append(r, v)
		}
	}

	return r
}

func (s *Set) Strings() []string {
	all := s.Value()

	r := make([]string, len(all))

	for i, v := range all {
		r[i] = string(v)
	}

	return r
}

func (s *Set) Add(add []byte) *Set {
	// Make sure that our set doesn't already contain this value
	for _, item := range s.value {
		if bytes.Equal(item, add) {
			return s
		}
	}

	// Add to s.value
	s.value = append(s.value, add)

	// Add to s.adds (Riak actions not yet saved)
	s.adds = append(s.adds, add)

	// Remove from s.removes
	for i, item := range s.removes {
		if bytes.Equal(item, add) {

			// https://github.com/golang/go/wiki/SliceTricks
			s.removes[i] = s.removes[len(s.removes)-1]
			s.removes = s.removes[:len(s.removes)-1]
		}
	}

	return s
}

func (s *Set) Remove(remove []byte) *Set {

	// Remove from s.value
	for i, item := range s.value {
		if bytes.Equal(item, remove) {

			// https://github.com/golang/go/wiki/SliceTricks
			s.value[i] = s.value[len(s.value)-1]
			s.value = s.value[:len(s.value)-1]
		}
	}

	// Remove from s.adds
	for i, item := range s.adds {
		if bytes.Equal(item, remove) {

			// https://github.com/golang/go/wiki/SliceTricks
			s.adds[i] = s.adds[len(s.adds)-1]
			s.adds = s.adds[:len(s.adds)-1]
		}
	}

	// Add to s.removes
	s.removes = append(s.removes, remove)

	return s
}

func (s *Set) AddString(add string) *Set {
	return s.Add([]byte(add))
}

func (s *Set) RemoveString(remove string) *Set {
	return s.Remove([]byte(remove))
}

func (s *Set) Exec(client *Client) error {
	if s == nil {
		return errors.New("Nil Set")
	}

	if s.name == "" {
		return errors.New("Unknown path to Set. Retreive Set with GetMap before updating the Set")
	}

	op := &riak.MapOperation{}
	outerOp := op

	// Traverse c.path so that we increment the correct counter in nested maps
	for _, subMapName := range s.path {
		op = op.Map(subMapName)
	}

	// Perform Add actions
	for _, val := range s.adds {
		op.AddToSet(s.name, val)
	}

	// Perform Remove actions
	for _, val := range s.removes {
		op.RemoveFromSet(s.name, val)
	}

	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucket(s.key.bucket).
		WithBucketType(s.key.bucketType).
		WithKey(s.key.key).
		WithMapOperation(outerOp).
		WithContext(s.context).
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
