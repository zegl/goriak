package goriak

import (
	"reflect"
	"testing"
)

func TestInsertAndGet(t *testing.T) {
	c, _ := Connect(ConnectOpts{
		Address: "127.0.0.1",
	})

	type ourTestType struct {
		A string
		B []string
	}

	val := ourTestType{
		A: "AAA",
		B: []string{"B", "BB", "BBB"},
	}

	res, err := Bucket("testsuitemap", "maps").Set(val).Run(c)

	if err != nil {
		t.Error("Insert:", err)
	}

	if len(res.Key) < 20 {
		t.Error("Unexpected key")
	}

	var out ourTestType
	res, err = Bucket("testsuitemap", "maps").Get(res.Key, &out).Run(c)

	if err != nil {
		t.Error("Get:", err)
	}

	if !reflect.DeepEqual(val, out) {
		t.Error("Not equal")
	}
}
