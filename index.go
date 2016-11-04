package goriak

import (
	riak "github.com/basho/riak-go-client"
)

type SecondaryIndexQueryResult struct {
	Key        string
	IsComplete bool
}

func (c Command) KeysInIndex(indexName, indexValue string, callback func(SecondaryIndexQueryResult)) Command {

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

	if c.limit != 0 {
		builder.WithMaxResults(c.limit)
	}

	cmd, err := builder.Build()

	if err != nil {
		c.err = err
		return c
	}

	c.commandType = riakSecondaryIndexQueryCommand
	c.riakCommand = cmd

	return c
}
