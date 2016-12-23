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
		t.Error("Set:", err)
	}

	var v map[string]string
	res, err = Bucket("json", "default").GetJSON(res.Key, &v).Run(con())

	if err != nil {
		t.Error("Get:", err)
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

func TestJSONSetIndex(t *testing.T) {
	type testType struct {
		User string
		Age  string
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
		_, err := Bucket("json", "default").
			AddToIndex("ageC_bin", u.Age).
			SetJSON(u).
			Run(con())

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
	Bucket("json", "default").Limit(2).KeysInIndex("ageC_bin", "10", cb).Run(con())

	if foundCount != 2 {
		t.Error("Expected 2 results, got: ", foundCount)
	}

	foundCount = 0

	// Unlimited
	Bucket("json", "default").KeysInIndex("ageC_bin", "10", cb).Run(con())

	if foundCount != 4 {
		t.Error("Expected 4 results, got: ", foundCount)
	}
}

func TestJSONWithSliceIndex(t *testing.T) {
	type testType struct {
		User string
		Ages []string `goriakindex:"ageslice_bin"`
	}

	users := []testType{
		{"A", []string{"10", "11"}},
		{"B", []string{"10", "11"}},
		{"C", []string{"10", "11"}},
		{"D", []string{"10", "11"}},
		{"E", []string{"13", "11"}},
		{"F", []string{"13", "11"}},
		{"G", []string{"13", "11"}},
		{"H", []string{"13", "11"}},
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
	Bucket("json", "default").Limit(2).KeysInIndex("ageslice_bin", "10", cb).Run(con())

	if foundCount != 2 {
		t.Error("1: Expected 2 results, got: ", foundCount)
	}

	foundCount = 0

	// With limit other order
	Bucket("json", "default").KeysInIndex("ageslice_bin", "10", cb).Limit(2).Run(con())

	if foundCount != 2 {
		t.Error("2: Expected 2 results, got: ", foundCount)
	}

	foundCount = 0

	// Unlimited
	Bucket("json", "default").KeysInIndex("ageslice_bin", "10", cb).Run(con())

	if foundCount != 4 {
		t.Error("3: Expected 4 results, got: ", foundCount)
	}
}

func TestSetJSONKeyAfterSet(t *testing.T) {
	c := con()

	res, err := Bucket("json", "default").SetJSON(123).Key("json-set-test").Run(c)

	if res.Key != "json-set-test" {
		t.Error("Unknown key (1)")
	}

	if err != nil {
		t.Error(err)
	}

	var output int
	res, err = Bucket("json", "default").GetJSON("json-set-test", &output).Run(c)

	if output != 123 {
		t.Error("Output was not set to 123")
	}

	if res.Key != "json-set-test" {
		t.Error("Unknown key (2)")
	}

	if err != nil {
		t.Error(err)
	}
}

func TestJSONIntIndex(t *testing.T) {
	type testType struct {
		User string
		Age  int `goriakindex:"ageint_int"`
	}

	users := []testType{
		{"A", 10},
		{"B", 10},
		{"C", 10},
		{"D", 10},
		{"E", 13},
		{"F", 13},
		{"G", 13},
		{"H", 13},
	}
	for _, u := range users {
		_, err := Bucket("json", "default").
			SetJSON(u).
			Run(con())

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
	Bucket("json", "default").Limit(2).KeysInIndex("ageint_int", "10", cb).Run(con())

	if foundCount != 2 {
		t.Error("Expected 2 results, got: ", foundCount)
	}

	foundCount = 0

	// Unlimited
	Bucket("json", "default").KeysInIndex("ageint_int", "10", cb).Run(con())

	if foundCount != 4 {
		t.Error("Expected 4 results, got: ", foundCount)
	}
}

func TestJSONIntSliceIndex(t *testing.T) {
	type testType struct {
		User string
		Age  []int `goriakindex:"ageintslice_int"`
	}

	users := []testType{
		{"A", []int{10}},
		{"B", []int{10}},
		{"C", []int{10}},
		{"D", []int{10}},
		{"E", []int{13}},
		{"F", []int{13}},
		{"G", []int{13}},
		{"H", []int{13}},
	}
	for _, u := range users {
		_, err := Bucket("json", "default").
			SetJSON(u).
			Run(con())

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
	Bucket("json", "default").Limit(2).KeysInIndex("ageintslice_int", "10", cb).Run(con())

	if foundCount != 2 {
		t.Error("Expected 2 results, got: ", foundCount)
	}

	foundCount = 0

	// Unlimited
	Bucket("json", "default").KeysInIndex("ageintslice_int", "10", cb).Run(con())

	if foundCount != 4 {
		t.Error("Expected 4 results, got: ", foundCount)
	}
}

func TestJSONUnknownSliceIndex(t *testing.T) {
	type testType struct {
		User string
		Age  []byte `goriakindex:"ageintbyteslice_int"`
	}

	_, err := Bucket("json", "default").
		SetJSON(testType{
			User: "Yay",
			Age:  []byte{100, 200},
		}).
		Run(con())

	if err == nil {
		t.Error("no error")
		return
	}

	if err.Error() != "Did not know how to set index: Age" {
		t.Error("Unexpected error:", err.Error())
	}
}

func TestJSONUnknownIndex(t *testing.T) {
	type testType struct {
		User string
		Age  byte `goriakindex:"ageintbyte_int"`
	}

	_, err := Bucket("json", "default").
		SetJSON(testType{
			User: "Yay2",
			Age:  130,
		}).
		Run(con())

	if err == nil {
		t.Error("no error")
		return
	}

	if err.Error() != "Did not know how to set index: Age" {
		t.Error("Unexpected error:", err.Error())
	}
}
