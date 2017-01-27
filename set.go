package goriak

import (
	"bytes"
	"encoding/json"
	"errors"

	riak "github.com/basho/riak-go-client"
)

type helper struct {
	path    []string    // Path to the counter (can be a map in a map in a map, etc..)
	name    string      // Name of the counter
	key     requestData // bucket information
	context []byte      // riak context
}

// NewSet returnes a new and empty Set.
// Sets returned from NewSet() can not be used with Set.Exec()
func NewSet() *Set {
	return &Set{}
}

// Set is a special type to make it easier to work with Riak Sets in Go.
type Set struct {
	helper

	value   [][]byte // The full content
	adds    [][]byte // Not-yet performed Add actions (performed locally but not to Riak)
	removes [][]byte // Same as adds, but for removal of items
}

// Value returnes the raw values from the Set
func (s *Set) Value() [][]byte {
	return s.value
}

// Strings returns the same data as Value(), but encoded as strings
func (s *Set) Strings() []string {
	all := s.Value()

	r := make([]string, len(all))

	for i, v := range all {
		r[i] = string(v)
	}

	return r
}

// Add adds an item to the direct value of the Set.
// Save the changes to Riak with Set.Exec() or SetMap().
func (s *Set) Add(add []byte) *Set {
	// Do not allow empty items (for backwards compatibility with goriak < 2.4)
	if len(add) == 0 {
		return s
	}

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

// Remove deletes an item to the direct value of the Set.
// Save the changes to Riak with Set.Exec() or SetMap().
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

// AddString is a shortcut to Add
func (s *Set) AddString(add string) *Set {
	return s.Add([]byte(add))
}

// RemoveString is a shortcut to Remove
func (s *Set) RemoveString(remove string) *Set {
	return s.Remove([]byte(remove))
}

// HasString returns true if search is a value in the set
func (s *Set) HasString(search string) bool {
	return s.Has([]byte(search))
}

// Has returns true if search is a value in the set
func (s *Set) Has(search []byte) bool {
	for _, item := range s.value {
		if bytes.Equal(item, search) {
			return true
		}
	}

	return false
}

// Exec executes the diff created by Add() and Remove(), and saves the data to Riak
func (s *Set) Exec(client *Session) error {
	if s == nil {
		return errors.New("Nil Set")
	}

	if s.name == "" {
		return errors.New("Unknown path to Set. Retrieve Set with Get or Set before updating the Set")
	}

	// Validate s.key
	if s.key.bucket == "" || s.key.bucketType == "" || s.key.key == "" {
		return errors.New("Invalid key in Set Exec()")
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
		WithReturnBody(true).
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

	// Update internal status
	resMap := res.Response.Map

	for _, subMapName := range s.path {
		resMap = resMap.Maps[subMapName]
	}

	s.value = resMap.Sets[s.name]
	s.context = res.Response.Context

	return nil
}

// MarshalJSON satisfies the JSON interface
func (s Set) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.value)
}

// UnmarshalJSON satisfies the JSON interface
func (s *Set) UnmarshalJSON(data []byte) error {
	var values [][]byte

	err := json.Unmarshal(data, &values)

	if err != nil {
		return err
	}

	s.value = values
	return nil
}

func (s *Set) removeEmptyItems() {
	for i, v := range s.value {
		if len(v) == 0 {
			// Remove without preserving order
			s.value[i] = s.value[len(s.value)-1]
			s.value[len(s.value)-1] = nil
			s.value = s.value[:len(s.value)-1]
		}
	}
}
