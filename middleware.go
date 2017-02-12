package goriak

type ExecMiddlewarer interface {
	GetKey() string
}

type ExecuteMiddleware func(cmd ExecMiddlewarer, next func() (*Result, error)) (*Result, error)

func (c *SetRawCommand) AddMiddleware(mid ExecuteMiddleware) *SetRawCommand {
	c.execMiddleware = append(c.execMiddleware, mid)
	return c
}

func (c *SetRawCommand) GetKey() string {
	return c.key
}
