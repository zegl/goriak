package goriak

import (
	riak "github.com/basho/riak-go-client"
)

func connect() *riak.Client {
	con, err := riak.NewClient(&riak.NewClientOptions{
		RemoteAddresses: []string{"127.0.0.1"},
	})

	if err != nil {
		panic(err)
	}

	return con
}
