package goriak

import (
	"reflect"
	"testing"
)

func TestSetJSON(t *testing.T) {

	data := map[string]string{
		"Key": "Value",
		"AA":  "BB",
	}

	res, err := Bucket("json", "default").SetJSON(data).Run(con())

	if err != nil {
		t.Error(err)
	}

	var v map[string]string
	res, err = Bucket("json", "default").Key(res.Key).GetJSON(&v).Run(con())

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(data, v) {
		t.Error("Not equal")
	}
}

func TestSetJSONWithIndexes(t *testing.T) {
	type testType struct {
		Username string `goriakindex:"username_bin"`
		Name     string
	}

	val := testType{
		Username: "zegl",
		Name:     "Gustav",
	}

	setresult, err := Bucket("json", "default").SetJSON(val).Run(con())

	if err != nil {
		t.Error(err)
		return
	}

	foundCount := 0
	foundCorrent := false

	cb := func(key SecondaryIndexQueryResult) {
		if !key.IsComplete {
			foundCount++
		}

		if key.Key == setresult.Key {
			foundCorrent = true
		}

	}

	Bucket("json", "default").KeysInIndex("username_bin", "zegl", cb).Run(con())

	if foundCount != 1 {
		t.Error("Expected to find 1 item, found ", foundCount)
	}

	if !foundCorrent {
		t.Error("Did not find the correct item")
	}
}
