package goriak

// WithPw sets the amount of primary nodes required to report back during writes.
// Used with Set(), SetRaw(), SetJSON(), and Delete().
func (c Command) WithPw(pw uint32) Command {
	c.riakPW = pw
	return c
}

// WithDw sets the amount of nodes required to report sucessfully writes to backend storage.
// Used with Set(), SetRaw(), SetJSON(), and Delete()
func (c Command) WithDw(dw uint32) Command {
	c.riakDW = dw
	return c
}

// WithW sets the amount of nodes required to report back during writes.
// Used with Set(), SetRaw(), SetJSON(), and Delete().
func (c Command) WithW(w uint32) Command {
	c.riakW = w
	return c
}

// WithRr sets the amount fo nodes required to report successfull deletes from backend storage.
func (c Command) WithRr(rw uint32) Command {
	c.riakRW = rw
	return c
}

// WithPr sets the amount of primary nodes required to report back during reads.
// Used with Get(), GetRaw(), and GetJSON().
func (c Command) WithPr(pr uint32) Command {
	c.riakPR = pr
	return c
}

// WithR sets the amount of nodes required to report back during reads.
// Used with Get(), GetRaw(), and GetJSON().
func (c Command) WithR(r uint32) Command {
	c.riakR = r
	return c
}
