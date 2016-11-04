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

func TestJSONWithIndexLimits(t *testing.T) {
	type testType struct {
		User string
		Age  string `goriakindex:"age_bin"`
	}

	users := []testType{
		{"A", "10"},
		{"B", "10"},
		{"C", "10"},
		{"D", "10"},
		{"E", "13"},
		{"F", "13"},
		{"G", "13"},
		{"H", "13"},
	}

	for _, u := range users {
		_, err := Bucket("json", "default").SetJSON(u).Run(con())

		if err != nil {
			t.Error(err)
			return
		}
	}

	foundCount := 0

	cb := func(key SecondaryIndexQueryResult) {
		if !key.IsComplete {
			foundCount++
		}
	}

	// With limit
	Bucket("json", "default").Limit(2).KeysInIndex("age_bin", "10", cb).Run(con())

	if foundCount != 2 {
		t.Error("Expected 2 results, got: ", foundCount)
	}

	foundCount = 0

	// Unlimited
	Bucket("json", "default").KeysInIndex("age_bin", "10", cb).Run(con())

	if foundCount != 4 {
		t.Error("Expected 4 results, got: ", foundCount)
	}
}
