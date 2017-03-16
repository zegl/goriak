package goriak

type RunMiddlewarer interface {
	Key() string
	Bucket() string
	BucketType() string
	Command() CommandType
}

type CommandType uint8

const (
	CommandTypeGet CommandType = 1
	CommandTypeSet             = 2
	// GetRaw() and GetJSON()
	CommandTypeGetRaw = 3
	// SetRaw() and SetJSON()
	CommandTypeSetRaw      = 4
	CommandTypeKeysInIndex = 5
)

type RunMiddleware func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error)

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

func (c setRawMiddlewarer) Command() CommandType {
	return CommandTypeSetRaw
}

type getRawMiddlewarer struct {
	cmd *GetRawCommand
}

func (c getRawMiddlewarer) Key() string {
	return c.cmd.key
}

func (c getRawMiddlewarer) Bucket() string {
	return c.cmd.bucket
}

func (c getRawMiddlewarer) BucketType() string {
	return c.cmd.bucketType
}

func (c getRawMiddlewarer) Command() CommandType {
	return CommandTypeGetRaw
}
