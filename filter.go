package goriak

// FilterInclude adds a include filter to Set().
// Call FilterInclude("A") to only include the field A (and children).
// Can be combined with FilterExclude() to form more complicated patterns.
// Use FilterInclude or FilterExclude without parameters to include/exclude the root object.
// If the same field is both included and excluded the include is prioritized.
func (c *commandMapSet) FilterInclude(path ...string) *commandMapSet {
	c.includeFilter = append(c.includeFilter, path)
	return c
}

// FilterExclude does the opposite of FilterInclude.
// See FinterInclude for more info.
func (c *commandMapSet) FilterExclude(path ...string) *commandMapSet {
	c.excludeFilter = append(c.excludeFilter, path)
	return c
}

func (c *commandMapSet) filterAllowPath(path ...string) bool {

	// No filter has been set: Allow all paths
	if len(c.includeFilter) == 0 && len(c.excludeFilter) == 0 {
		return true
	}

	allowedByIncludeFilter := false
	allowedLevel := 0

	disallowedByExcludeFilter := false
	disallowedLevel := 0

	// Check if in include filter
	for _, include := range c.includeFilter {
		// Can not be allowed by a filter shorter than us
		if len(path) < len(include) {
			continue
		}

		allValid := true
		for i, p := range include {
			if path[i] != p {
				allValid = false
			}
		}

		if allValid {
			allowedByIncludeFilter = true

			if len(include) > allowedLevel {
				allowedLevel = len(include)
			}
		}
	}

	// Check if in exclude filter
	for _, exclude := range c.excludeFilter {
		// Can not be allowed by a filter shorter than us
		if len(path) < len(exclude) {
			continue
		}

		allValid := true
		for i, p := range exclude {
			if path[i] != p {
				allValid = false
			}
		}

		if allValid {
			disallowedByExcludeFilter = true

			if len(exclude) > disallowedLevel {
				disallowedLevel = len(exclude)
			}
		}
	}

	if allowedByIncludeFilter && disallowedByExcludeFilter {
		if allowedLevel >= disallowedLevel {
			return true
		}

		return false
	}

	if allowedByIncludeFilter {
		return true
	}

	if disallowedByExcludeFilter {
		return false
	}

	// Include by default
	if len(c.includeFilter) == 0 {
		return true
	}

	return false
}
