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
	res, err = Bucket("json", "default").Key(res.Key).GetRaw(&v).Run(con())

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
			AddToIndex("testA_bin", "hello").
			AddToIndex("testB_bin", "world").
			SetRaw([]byte(s)).
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
