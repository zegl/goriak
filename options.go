package goriak

func (c Command) AddToIndex(key, value string) Command {
	// Create map if needed
	if c.indexes == nil {
		c.indexes = make(map[string][]string)
	}

	// Add to existing slice
	if _, ok := c.indexes[key]; ok {
		c.indexes[key] = append(c.indexes[key], value)
		return c
	}

	// Create new slice
	c.indexes[key] = []string{value}
	return c
}
