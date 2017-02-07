package goriak

import (
	//riak "github.com/basho/riak-go-client"
	"testing"
)

func TestMiddlewareSet(t *testing.T) {

	RegisterSetMiddleware(func(item SetMiddlewarer, next NextMiddlewareFunc) {

	})

	Bucket("middleware", "raw").
		SetRaw([]byte{1, 2, 3, 4, 5}).
		Run(con())
}
