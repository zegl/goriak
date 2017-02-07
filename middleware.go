package goriak

type SetMiddlewarer interface {
	GetValue() ([]byte, error)
	SetValue([]byte) error

	//GetMapOperation() (*riak.MapOperation, error)
	//SetMapOperation(*riak.MapOperation) error

	GetKey() string
	SetKey(string)
}

type SetMiddleware func(SetMiddlewarer, SetMiddleware)

var globalSetMiddleware []SetMiddleware

type NextMiddlewareFunc func()

func RegisterSetMiddleware(middleware SetMiddleware) {
	globalSetMiddleware = append(globalSetMiddleware, middleware)
}
