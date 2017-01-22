package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// SecondaryIndexQueryResult is the items sent to the callback function
// used by KeysInIndex
type SecondaryIndexQueryResult struct {
	Key        string
	IsComplete bool
}

type commandKeysInIndex struct {
	*Command

	builder *riak.SecondaryIndexQueryCommandBuilder
	limit   uint32
}

// KeysInIndex returns all keys in the index indexName that has the value indexValue
// The values will be returned to the callbak function
// When all keys have been returned SecondaryIndexQueryResult.IsComplete will be true
func (c *Command) KeysInIndex(indexName, indexValue string, callback func(SecondaryIndexQueryResult)) *commandKeysInIndex {

	cb := func(res []*riak.SecondaryIndexQueryResult) error {
		if len(res) == 0 {
			callback(SecondaryIndexQueryResult{
				Key:        "",
				IsComplete: true,
			})
		}

		for _, i := range res {
			callback(SecondaryIndexQueryResult{
				Key:        string(i.ObjectKey),
				IsComplete: false,
			})
		}

		return nil
	}

	builder := riak.NewSecondaryIndexQueryCommandBuilder().
		WithBucket(c.bucket).
		WithBucketType(c.bucketType).
		WithIndexName(indexName).
		WithIndexKey(indexValue).
		WithStreaming(true).
		WithCallback(cb)

	return &commandKeysInIndex{
		Command: c,
		builder: builder,
	}
}

// Limit sets the limit returned in KeysInIndex
// A limit of 0 means unlimited
func (c *commandKeysInIndex) Limit(limit uint32) *commandKeysInIndex {
	c.limit = limit
	return c
}
