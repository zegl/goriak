package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
)

// SecondaryIndexQueryResult is the items sent to the callback function
// used by KeysInIndex
type SecondaryIndexQueryResult struct {
	Key        string
	IsComplete bool
}

type CommandKeysInIndex struct {
	*Command

	builder *riak.SecondaryIndexQueryCommandBuilder
}

// KeysInIndex returns all keys in the index indexName that has the value indexValue
// The values will be returned to the callbak function
// When all keys have been returned SecondaryIndexQueryResult.IsComplete will be true
func (c *Command) KeysInIndex(indexName, indexValue string, callback func(SecondaryIndexQueryResult)) *CommandKeysInIndex {

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

	return &CommandKeysInIndex{
		Command: c,
		builder: builder,
	}
}

// Limit sets the limit returned in KeysInIndex
// A limit of 0 means unlimited
func (c *CommandKeysInIndex) Limit(limit uint32) *CommandKeysInIndex {
	c.builder.WithMaxResults(limit)
	return c
}

// buildSecondaryIndexQueryCommand completes the buildinf of the SecondaryIndexQueryCommand used by KeysInIndex
func (c *CommandKeysInIndex) Run(session *Session) (*Result, error) {

	// Build it!
	cmd, err := c.builder.Build()
	if err != nil {
		return nil, err
	}

	err = session.riak.Execute(cmd)
	if err != nil {
		return nil, err
	}

	res := cmd.(*riak.SecondaryIndexQueryCommand)

	if !res.Success() {
		return nil, errors.New("not successful")
	}

	return &Result{}, nil
}
