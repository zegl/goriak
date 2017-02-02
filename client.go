package goriak

import (
	riak "github.com/basho/riak-go-client"

	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"strconv"
	"strings"
)

// Session holds the connection to Riak
type Session struct {
	riak *riak.Cluster
	opts ConnectOpts
}

// ConnectOpts are the available options for connecting to your Riak instance
type ConnectOpts struct {
	// Both Address and Addresses should be on the form HOST|IP[:PORT]
	Address   string   // Address to a single Riak host. Will be used in case Addresses is empty
	Addresses []string // Addresses to all Riak hosts.

	// Username and password for connection to servers with secirity enabled
	User     string
	Password string

	// Path to root CA certificate. Required if security is used
	CARootCert string

	// Option to override port. Is set to 8087 by default
	Port uint32
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

	var authOptions *riak.AuthOptions

	// Build auth options
	if c.opts.User != "" {

		rootCertPemData, err := ioutil.ReadFile(c.opts.CARootCert)
		if err != nil {
			return errors.New("Opening CARootCert: " + err.Error())
		}

		rootCertPool := x509.NewCertPool()
		if !rootCertPool.AppendCertsFromPEM(rootCertPemData) {
			return errors.New("Invalid PEM certificate file")
		}

		tlsConf := &tls.Config{
			ServerName:         "localhost",
			InsecureSkipVerify: true,
			RootCAs:            rootCertPool,
		}

		authOptions = &riak.AuthOptions{
			User:      c.opts.User,
			Password:  c.opts.Password,
			TlsConfig: tlsConf,
		}
	}

	var nodes []*riak.Node

	// Set to default port if not provided
	port := c.opts.Port
	if port == 0 {
		port = 8087
	}

	for _, address := range c.opts.Addresses {
		if !strings.Contains(address, ":") {
			// Add port if not set in the user config
			address = address + ":" + strconv.FormatUint(uint64(port), 10)
		}

		node, err := riak.NewNode(&riak.NodeOptions{
			RemoteAddress: address,
			AuthOptions:   authOptions,
		})
		if err != nil {
			return err
		}

		nodes = append(nodes, node)
	}

	con, err := riak.NewCluster(&riak.ClusterOptions{
		Nodes: nodes,
	})
	if err != nil {
		return err
	}

	err = con.Start()
	if err != nil {
		return err
	}

	c.riak = con

	return nil
}
