package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// Session holds the connection to Riak
type Session struct {
	riak *riak.Client
	opts ConnectOpts
}

// ConnectOpts are the available options for connecting to your Riak instance
type ConnectOpts struct {
	// Both Adress and Addresses should be on the form HOST|IP[:PORT]
	Address   string   // Address to a single Riak host. Will be used in case Addresses is empty
	Addresses []string // Addresses to all Riak hosts.
}

// Connect creates a new Riak connection. See ConnectOpts for the available options.
func Connect(opts ConnectOpts) (*Session, error) {
	client := Session{
		opts: opts,
	}

	err := client.connect()

	if err != nil {
		return nil, err
	}

	return &client, nil
}

func (c *Session) connect() error {
	if len(c.opts.Addresses) == 0 {
		c.opts.Addresses = []string{c.opts.Address}
	}

	con, err := riak.NewClient(&riak.NewClientOptions{
		RemoteAddresses: c.opts.Addresses,
	})

	if err != nil {
		return err
	}

	c.riak = con

	return nil
}
