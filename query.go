package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

// Command is the main query builder object
type Command struct {
	// Key information
	bucket     string
	bucketType string

	// Limit result for SecondaryIndex queries
	limit uint32

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

	// Filters
	includeFilter [][]string
	excludeFilter [][]string
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
