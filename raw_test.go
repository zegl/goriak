package goriak

import (
	"reflect"
	"sort"
	"testing"
)

func TestSetRaw(t *testing.T) {
	data := []byte{1, 10, 2, 9, 3, 8}

	res, err := Bucket("json", "default").SetRaw(data).Run(con())

	if err != nil {
		t.Error(err)
	}

	var v []byte
	res, err = Bucket("json", "default").GetRaw(res.Key, &v).Run(con())

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, v) {
		t.Error("Not equal")
	}
}

func TestSetRawWithIndex(t *testing.T) {
	c := con()

	strs := []string{"Hello", "Hej", "Hallo"}

	var keys []string

	for _, s := range strs {
		res, err := Bucket("json", "default").
			SetRaw([]byte(s)).
			AddToIndex("testA_bin", "hello").
			AddToIndex("testB_bin", "world").
			Run(c)

		if err != nil {
			t.Error(err)
		}

		keys = append(keys, res.Key)
	}

	var foundKeys []string

	cb := func(r SecondaryIndexQueryResult) {
		if !r.IsComplete {
			foundKeys = append(foundKeys, r.Key)
		}
	}

	Bucket("json", "default").KeysInIndex("testA_bin", "hello", cb).Run(c)

	sort.Strings(keys)
	sort.Strings(foundKeys)

	if !reflect.DeepEqual(keys, foundKeys) {
		t.Log("Exected:", keys)
		t.Log("Found:", foundKeys)
		t.Error("Not equal 1")
	}

	foundKeys = []string{}

	Bucket("json", "default").KeysInIndex("testB_bin", "world", cb).Run(c)

	sort.Strings(foundKeys)

	if !reflect.DeepEqual(keys, foundKeys) {
		t.Log("Exected:", keys)
		t.Log("Found:", foundKeys)
		t.Error("Not equal 2")
	}
}

func TestSetRawAddToMultipleIndexes(t *testing.T) {
	c := con()

	res, err := Bucket("json", "default").
		SetRaw([]byte("fooooobar")).
		AddToIndex("testC_bin", "foo").
		AddToIndex("testC_bin", "bar").
		Run(c)

	if err != nil {
		t.Error(err)
		return
	}

	foundCount := 0
	foundCorrect := false

	cb := func(r SecondaryIndexQueryResult) {
		if !r.IsComplete {
			foundCount++
			if r.Key == res.Key {
				foundCorrect = true
			}
		}
	}

	_, err = Bucket("json", "default").KeysInIndex("testC_bin", "foo", cb).Run(c)

	if err != nil {
		t.Error(err)
	}

	if foundCount != 1 {
		t.Error("Unexpected count 1. Expected 1, got ", foundCount)
	}

	if !foundCorrect {
		t.Error("Did not find the correct value 1")
	}

	foundCount = 0
	foundCorrect = false

	Bucket("json", "default").KeysInIndex("testC_bin", "bar", cb).Run(c)

	if err != nil {
		t.Error(err)
	}

	if foundCount != 1 {
		t.Error("Unexpected count 2. Expected 1, got ", foundCount)
	}

	if !foundCorrect {
		t.Error("Did not find the correct value 2")
	}

}
