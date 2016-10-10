package goriak

import (
	"testing"
)

type ourID [10]byte

type objWithCustomType struct {
	ID  ourID
	Val string
}

func TestCustomType(t *testing.T) {
	id := ourID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	o := objWithCustomType{
		ID:  id,
		Val: "Custom1",
	}

	err := SetMap("customtype", "maps", "cus1", o)

	if err != nil {
		t.Error("Set:", err)
	}

	var res objWithCustomType
	err, isNotFound := GetMap("customtype", "maps", "cus1", &res)

	if err != nil {
		t.Error("Get:", err)
	}

	if isNotFound {
		t.Error("Not found")
	}

	if res.ID != id {
		t.Error("Did not get the same value of ID")
		t.Error(res.ID)
	}

	if res.Val != "Custom1" {
		t.Error("Val was not Custom1")
	}
}
