package goriak

import (
	//riak "github.com/basho/riak-go-client"
	"errors"
	"log"
	"testing"
)

func TestMiddlewareSet(t *testing.T) {

	m := func(cmd ExecMiddlewarer, next func() (*Result, error)) (*Result, error) {
		log.Println("Middleware BEFORE:", cmd.GetKey())
		return nil, errors.New("abort!")

		res, err := next()
		log.Println("Middleware After:", cmd.GetKey())
		return res, err
	}

	res, err := Bucket("middleware", "tests").
		SetRaw([]byte{1, 2, 3, 4, 5}).
		///Key("abc123").
		AddMiddleware(m).
		AddMiddleware(m).
		Run(con())

	t.Log(res, err)
}
