package goriak

import (
	"testing"
)

func TestFilterIncludeSingle(t *testing.T) {
	setup := bucket().Set(nil).FilterInclude("A")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: true},
		{path: []string{"A", "B"}, expected: true},
		{path: []string{"B"}, expected: false},
		{path: []string{"B", "C"}, expected: false},
		{path: []string{""}, expected: false},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterIncludeDouble(t *testing.T) {
	setup := bucket().Set(nil).FilterInclude("A", "B").FilterInclude("C")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: false},
		{path: []string{"A", "B"}, expected: true},
		{path: []string{"B"}, expected: false},
		{path: []string{"B", "C"}, expected: false},
		{path: []string{"C"}, expected: true},
		{path: []string{"C", "D", "E"}, expected: true},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterExclude(t *testing.T) {
	setup := bucket().Set(nil).FilterExclude("B")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: true},
		{path: []string{"B"}, expected: false},
		{path: []string{"C"}, expected: true},
		{path: []string{"B", "C"}, expected: false},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterExclude2(t *testing.T) {
	setup := bucket().Set(nil).FilterExclude("B", "C")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: true},
		{path: []string{"B"}, expected: true},
		{path: []string{"C"}, expected: true},
		{path: []string{"B", "C"}, expected: false},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterInludeWithExclude(t *testing.T) {
	setup := bucket().Set(nil).
		FilterInclude("A").
		FilterExclude("A", "B")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: true},
		{path: []string{"B"}, expected: false},
		{path: []string{"A", "B"}, expected: false},
		{path: []string{"A", "C"}, expected: true},
		{path: []string{"A", "C", "D"}, expected: true},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterExcludeWithInclude(t *testing.T) {
	setup := bucket().Set(nil).
		FilterInclude().
		FilterExclude("A").
		FilterInclude("A", "B")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: false},
		{path: []string{"B"}, expected: true},
		{path: []string{"B", "B"}, expected: true},
		{path: []string{"A", "B"}, expected: true},
		{path: []string{"A", "C"}, expected: false},
		{path: []string{"A", "C", "D"}, expected: false},
		{path: []string{"A", "B", "D"}, expected: true},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterIncludeAndExcludeSave(t *testing.T) {
	setup := bucket().Set(nil).
		FilterExclude("A").
		FilterInclude("A")

	type check struct {
		path     []string
		expected bool
	}

	tests := []check{
		{path: []string{"A"}, expected: true},
		{path: []string{"A", "B"}, expected: true},
		{path: []string{"B"}, expected: false},
	}

	for _, test := range tests {
		if setup.filterAllowPath(test.path...) != test.expected {
			t.Errorf("Unexpected: %+v", test.path)
		}
	}
}

func TestFilterSet(t *testing.T) {
	type item struct {
		A string
		B string
	}

	res, err := bucket().Set(item{
		A: "A",
		B: "B",
	}).FilterInclude("A").Run(con())
	if err != nil {
		t.Error(err)
	}

	var val item
	_, err = bucket().Get(res.Key, &val).Run(con())
	if err != nil {
		t.Error(err)
	}

	if val.A == "A" && val.B == "" {
		return
	}

	t.Errorf("Unexpected: %+v", val)
}

func TestFilterSetNested(t *testing.T) {
	type item struct {
		A struct {
			AA string
			AB string
		}
		B string
	}

	i := item{}
	i.A.AA = "AA"
	i.A.AB = "AB"
	i.B = "B"

	res, err := bucket().Set(i).FilterInclude("A").Run(con())
	if err != nil {
		t.Error(err)
	}

	var val item
	_, err = bucket().Get(res.Key, &val).Run(con())
	if err != nil {
		t.Error(err)
	}

	if val.A.AA == "AA" && val.A.AB == "AB" && val.B == "" {
		return
	}

	t.Errorf("Unexpected: %+v", val)
}
