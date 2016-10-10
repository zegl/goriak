package goriak

import (
	"errors"

	riak "github.com/basho/riak-go-client"
)

func KeysInIndex(bucket, bucketType, indexName, indexValue string, limit uint32) ([]string, error) {
	result := []string{}

	cmd, err := riak.NewSecondaryIndexQueryCommandBuilder().
		WithBucket(bucket).
		WithBucketType(bucketType).
		WithIndexName(indexName).
		WithIndexKey(indexValue).
		WithMaxResults(limit).
		Build()

	if err != nil {
		return result, err
	}

	err = connect().Execute(cmd)

	if err != nil {
		return result, err
	}

	res, ok := cmd.(*riak.SecondaryIndexQueryCommand)

	if !ok {
		return result, errors.New("Could not convert")
	}

	if !res.Success() {
		return result, errors.New("Not successful")
	}

	result = make([]string, len(res.Response.Results))

	for i, v := range res.Response.Results {
		result[i] = string(v.ObjectKey)
	}

	return result, nil
}
