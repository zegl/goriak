package goriak

// buildStoreValueCommand completes the building if the StoreValueCommand used by SetRaw and SetJSON
func (c *Command) buildStoreValueCommand() *Command {
	// Set key
	if c.key != "" {
		c.storeValueCommandBuilder.WithKey(c.key)
	}

	// Add indexes to object if needed
	// Indexes from Command.AddToIndex()
	for indexName, values := range c.indexes {
		for _, val := range values {
			c.storeValueObject.AddToIndex(indexName, val)
		}
	}

	// Durable writes (to backend storage)
	if c.riakDW > 0 {
		c.storeValueCommandBuilder.WithDw(c.riakDW)
	}

	// Primary node writes
	if c.riakPW > 0 {
		c.storeValueCommandBuilder.WithPw(c.riakPW)
	}

	// Node writes
	if c.riakW > 0 {
		c.storeValueCommandBuilder.WithW(c.riakW)
	}

	// Set VClock
	if len(c.vclock) > 0 {
		c.storeValueCommandBuilder.WithVClock(c.vclock)
	}

	// Set object
	c.storeValueCommandBuilder.WithContent(c.storeValueObject)

	// Build it!
	c.riakCommand, c.err = c.storeValueCommandBuilder.Build()
	return c
}

// buildSecondaryIndexQueryCommand completes the buildinf of the SecondaryIndexQueryCommand used by KeysInIndex
func (c *Command) buildSecondaryIndexQueryCommand() *Command {
	// Set limit
	if c.limit != 0 {
		c.secondaryIndexQueryCommandBuilder.WithMaxResults(c.limit)
	}

	// Build it!
	c.riakCommand, c.err = c.secondaryIndexQueryCommandBuilder.Build()
	return c
}

func (c *Command) buildUpdateMapQueryCommand() *Command {
	if c.key != "" {
		c.updateMapCommandBuilder.WithKey(c.key)
	}

	// Durable writes (to backend storage)
	if c.riakDW > 0 {
		c.updateMapCommandBuilder.WithDw(c.riakDW)
	}

	// Primary node writes
	if c.riakPW > 0 {
		c.updateMapCommandBuilder.WithPw(c.riakPW)
	}

	// Node writes
	if c.riakW > 0 {
		c.updateMapCommandBuilder.WithW(c.riakW)
	}

	// Build it!
	c.riakCommand, c.err = c.updateMapCommandBuilder.Build()
	return c
}

func (c *Command) buildFetchValueCommand() *Command {

	// Primary node reads
	if c.riakPR > 0 {
		c.fetchValueCommandBuilder.WithPr(c.riakPR)
	}

	// Node reads
	if c.riakR > 0 {
		c.fetchValueCommandBuilder.WithR(c.riakR)
	}

	// Build it!
	c.riakCommand, c.err = c.fetchValueCommandBuilder.Build()
	return c
}

func (c *Command) buildDeleteValueCommand() *Command {

	// Primary node writes
	if c.riakPW > 0 {
		c.deleteValueCommandBuilder.WithPw(c.riakPW)
	}

	// Durable writes
	if c.riakDW > 0 {
		c.deleteValueCommandBuilder.WithDw(c.riakDW)
	}

	// Node writes
	if c.riakW > 0 {
		c.deleteValueCommandBuilder.WithW(c.riakW)
	}

	// Primary reads
	if c.riakPR > 0 {
		c.deleteValueCommandBuilder.WithPr(c.riakPR)
	}

	// Node reads
	if c.riakR > 0 {
		c.deleteValueCommandBuilder.WithR(c.riakR)
	}

	// Durable deletes
	if c.riakRW > 0 {
		c.deleteValueCommandBuilder.WithRw(c.riakRW)
	}

	// Build it!
	c.riakCommand, c.err = c.deleteValueCommandBuilder.Build()
	return c
}
