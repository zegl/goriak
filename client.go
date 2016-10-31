package goriak

import (
	riak "github.com/basho/riak-go-client"
)

type Client struct {
	riak *riak.Client
}

func NewGoriak(host string) (*Client, error) {
	client := Client{}
	err := client.connect(host)

	if err != nil {
		return nil, err
	}

	return &client, nil
}

func (c *Client) connect(host string) error {
	con, err := riak.NewClient(&riak.NewClientOptions{
		RemoteAddresses: []string{host},
	})

	if err != nil {
		return err
	}

	c.riak = con

	return nil
}
