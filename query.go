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
