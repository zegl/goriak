/*
Package goriak is a Golang driver for Riak KV. Goriak offers simple ways of binding your Go datatypes and values to Riak.

Goriaks speciallity is dealing with Riak KV Data Types (http://docs.basho.com/riak/kv/2.1.4/developing/data-types/) and
allowing Marshal/Unmarshal of Go structs into Riak Maps.
*/
package goriak

import (
	riak "github.com/basho/riak-go-client"
)

// For backwards compability reasons while refactoring
type Client struct {
	riak *riak.Client
}
