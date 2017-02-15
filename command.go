package goriak

// Command is the main query builder object
type Command struct {
	// Key information
	bucket     string
	bucketType string

	// Middleware
	runMiddleware []RunMiddleware
}

// Result contains your query result data from Run()
type Result struct {
	NotFound bool   // Wether or not the item was not found when using Get, GetJSON, or GetRaw.
	Key      string // Returns your automatically generated key when using Set, SetJSON, or SetRaw.
	Context  []byte // Returns the Riak Context used in map operations. Is set when using Get.
}

// Bucket specifies the bucket and bucket type that your following command will be performed on.
func Bucket(bucket, bucketType string) *Command {
	return &Command{
		bucket:     bucket,
		bucketType: bucketType,
	}
}

func (c *Command) RegisterRunMiddleware(middleware RunMiddleware) *Command {
	c.runMiddleware = append(c.runMiddleware, middleware)
	return c
}
