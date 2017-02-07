package goriak

import (
	"testing"
)

func TestHllBasics(t *testing.T) {
	res, err := Bucket("hll-test", "hlls").
		UpdateHyperLogLog().
		Add([]byte{1}).
		Add([]byte{2}).
		Add([]byte{3}).
		Add([]byte{4}).
		ReturnBody(true).
		Run(con())
	if err != nil {
		t.Error(err)
	}

	if res.Cardinality != 4 {
		t.Error("unexpected cardinality")
	}

	if res.NotFound == true {
		t.Error("not found was true")
	}

	// Get same
	res2, err := Bucket("hll-test", "hlls").
		GetHyperLogLog(res.Key).
		Run(con())

	if res2.Cardinality != 4 {
		t.Error("unexpected cardinality")
	}

	if res.NotFound == true {
		t.Error("not found was true")
	}

	if res2.Key != res.Key {
		t.Error("unexpected key")
	}

	// Add one more
	res3, err := Bucket("hll-test", "hlls").
		UpdateHyperLogLog().
		Add([]byte{5}).
		Key(res.Key).
		ReturnBody(true).
		Run(con())
	if err != nil {
		t.Error(err)
	}

	if res3.Cardinality != 5 {
		t.Error("unexpected cardinality")
	}

	if res3.NotFound == true {
		t.Error("not found was true")
	}

	if res3.Key != res.Key {
		t.Error("unexpected key")
	}
}

func TestHllWithoutReturn(t *testing.T) {
	res, err := Bucket("hll-test", "hlls").
		UpdateHyperLogLog().
		Add([]byte{1}).
		Run(con())

	if err != nil {
		t.Error(err)
	}

	if res != nil {
		t.Error("res was not nil")
	}
}

func TestHllAddMultiple(t *testing.T) {
	res, err := Bucket("hll-test", "hlls").
		UpdateHyperLogLog().
		AddMultiple([]byte{1}, []byte{2}, []byte{3}).
		AddMultiple([]byte{1}, []byte{2}, []byte{3}).
		ReturnBody(true).
		Run(con())

	if err != nil {
		t.Error(err)
	}

	if res == nil {
		t.Error("res was nil")
	}

	if res.Cardinality != 3 {
		t.Error("unexpected val")
	}
}
