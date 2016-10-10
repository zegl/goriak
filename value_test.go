package goriak

import (
	"os"

	"log"
	"testing"
)

// Cleanup Bucket
func TestMain(m *testing.M) {
	deleteAllIn("testsuite", "default")
	deleteAllIn("customtype", "maps")
	deleteAllIn("testsuitemap", "maps")

	os.Exit(m.Run())
}

func deleteAllIn(bucket, bucketType string) {
	keys, _ := AllKeys(bucket, bucketType)

	for _, key := range keys {
		log.Println("Delete:", key)
		Delete(bucket, bucketType, key)
	}
}

type teststoreobject struct {
	A string `goriakindex:"testindex_bin"`
	B int
}

func TestGetSetValue(t *testing.T) {
	err := SetValue("testsuite", "default", "val1", teststoreobject{
		A: "Abc",
		B: 10002,
	})

	if err != nil {
		t.Error("SetValue:", err)
		return
	}

	var res teststoreobject
	getErr := GetValue("testsuite", "default", "val1", &res)

	if getErr != nil {
		t.Error("GetValue:", err)
		return
	}

	if res.A != "Abc" {
		t.Error("Wrong A value")
	}

	if res.B != 10002 {
		t.Error("Wrong B value")
	}
}

func TestValueWithIndex(t *testing.T) {

	Delete("testsuite", "default", "val2")

	err := SetValue("testsuite", "default", "val2", teststoreobject{
		A: "HelloWorld",
		B: 10002,
	})

	if err != nil {
		t.Error("SetValue:", err)
		return
	}

	keys, err := KeysInIndex("testsuite", "default", "testindex_bin", "HelloWorld", 100)

	if len(keys) != 1 {
		t.Error("Did not receive exactly 1 key")
		t.Error(keys)
		return
	}

	if keys[0] != "val2" {
		t.Error("The wrong key was returned")
		return
	}
}

type testsliceindex struct {
	Thing   string
	Indexes []string `goriakindex:"sliceindex_bin"`
}

func TestValueWithSliceIndex(t *testing.T) {
	err := SetValue("testsuite", "default", "slice1", testsliceindex{
		Thing:   "Hello",
		Indexes: []string{"Hola", "Hej", "Halo"},
	})

	if err != nil {
		t.Error(err)
	}

	keys, err := KeysInIndex("testsuite", "default", "sliceindex_bin", "Hej", 100)

	if err != nil {
		t.Error(err)
	}

	if len(keys) != 1 || keys[0] != "slice1" {
		t.Error("1: Unexpected content")
		t.Errorf("%+v", keys)
	}

	keys, err = KeysInIndex("testsuite", "default", "sliceindex_bin", "Hola", 100)

	if err != nil {
		t.Error(err)
	}

	if len(keys) != 1 || keys[0] != "slice1" {
		t.Error("2: Unexpected content")
		t.Errorf("%+v", keys)
	}
}
