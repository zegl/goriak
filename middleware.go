package goriak

type ExecMiddlewarer interface {
	Key() string
	Bucket() string
	BucketType() string
}

type ExecuteMiddleware func(cmd ExecMiddlewarer, next func() (*Result, error)) (*Result, error)

func (c *SetRawCommand) AddMiddleware(mid ExecuteMiddleware) *SetRawCommand {
	c.execMiddleware = append(c.execMiddleware, mid)
	return c
}

type setRawMiddlewarer struct {
	cmd *SetRawCommand
}

func (c setRawMiddlewarer) Key() string {
	return c.cmd.key
}

func (c setRawMiddlewarer) Bucket() string {
	return c.cmd.c.bucket
}

func (c setRawMiddlewarer) BucketType() string {
	return c.cmd.c.bucketType
}
