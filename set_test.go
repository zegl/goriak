package goriak

import (
	"reflect"
	"sort"
	"testing"
)

func TestAutoMapSet(t *testing.T) {
	type ourTestType struct {
		Items *Set
	}

	testVal := ourTestType{}
	result, errset := bucket().Set(testVal).Run(con())

	if errset != nil {
		t.Error("Set:", errset)
	}

	// Get when empty
	var res ourTestType
	_, errget := bucket().Get(result.Key, &res).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}

	res.Items.AddString("aaa")
	res.Items.AddString("bbb")
	res.Items.AddString("ccc")

	s := res.Items.Strings()

	if len(s) != 3 {
		t.Error("Unexpected length")
	}

	expected := []string{"aaa", "bbb", "ccc"}

	if !reflect.DeepEqual(expected, res.Items.Strings()) {
		t.Log(expected)
		t.Log(res.Items.Strings())
		t.Error("Not deep equal first")
	}

	err := res.Items.Exec(con())

	if err != nil {
		t.Error("Exec1: ", err)
	}

	// Get after save
	var res2 ourTestType
	_, errget = bucket().Get(result.Key, &res2).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}

	if !reflect.DeepEqual(res2.Items.Value(), res.Items.Value()) {
		t.Error("Not deep equal after get")
	}

	// Remove
	err = res2.Items.RemoveString("bbb").Exec(con())

	if err != nil {
		t.Error("Exec2: ", err)
	}

	// Get after remove
	var res3 ourTestType
	_, errget = bucket().Get(result.Key, &res3).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}

	if !reflect.DeepEqual(res3.Items.Value(), res2.Items.Value()) {
		t.Log(res3.Items.Strings())
		t.Log(res2.Items.Strings())
		t.Error("Not deep equal after get 2")
	}
}

func TestAutoMapSetAddRemove(t *testing.T) {
	set := NewSet()

	set.AddString("1")
	set.AddString("2")
	set.AddString("3")

	expected := []string{"1", "2", "3"}

	if !reflect.DeepEqual(expected, set.Strings()) {
		t.Log(expected)
		t.Log(set.Strings())
		t.Error("Unexpected 1")
	}

	set.RemoveString("2")

	expected = []string{"1", "3"}

	if !reflect.DeepEqual(expected, set.Strings()) {
		t.Log(expected)
		t.Log(set.Strings())
		t.Error("Unexpected 2")
	}

	expectedAdds := [][]byte{[]byte("1"), []byte("3")}

	if !reflect.DeepEqual(expectedAdds, set.adds) {
		t.Log(expectedAdds)
		t.Log(set.adds)
		t.Error("Unexpected adds 1")
	}

	expectedRemoves := [][]byte{[]byte("2")}

	if !reflect.DeepEqual(expectedRemoves, set.removes) {
		t.Log(expectedRemoves)
		t.Log(set.removes)
		t.Error("Unexpected removes 1")
	}

	set.RemoveString("4")

	expectedRemoves = [][]byte{[]byte("2"), []byte("4")}

	if !reflect.DeepEqual(expectedRemoves, set.removes) {
		t.Log(expectedRemoves)
		t.Log(set.removes)
		t.Error("Unexpected removes 2")
	}

	set.AddString("4")

	expectedAdds = [][]byte{[]byte("1"), []byte("3"), []byte("4")}

	if !reflect.DeepEqual(expectedAdds, set.adds) {
		t.Log(expectedAdds)
		t.Log(set.adds)
		t.Error("Unexpected adds 2")
	}

	expectedRemoves = [][]byte{[]byte("2")}

	if !reflect.DeepEqual(expectedRemoves, set.removes) {
		t.Log(expectedRemoves)
		t.Log(set.removes)
		t.Error("Unexpected removes 3")
	}
}

func TestAutoMapMultipleSet(t *testing.T) {
	set := NewSet()

	set.AddString("hello")
	set.AddString("hello")
	set.AddString("hello")
	set.AddString("hello")

	expectedVal := []string{"hello"}
	expectedAdds := [][]byte{[]byte("hello")}

	if !reflect.DeepEqual(expectedVal, set.Strings()) {
		t.Error("Unexpected value")
	}

	if !reflect.DeepEqual(expectedAdds, set.adds) {
		t.Error("Unexpected adds")
	}
}

func TestAutoMapSetAddRemoveSetMap(t *testing.T) {

	type ourTestType struct {
		Tags *Set

		Context []byte `goriak:"goriakcontext"`
	}

	testVal := ourTestType{
		Tags: NewSet(),
	}

	testVal.Tags.AddString("one")
	testVal.Tags.AddString("two")
	testVal.Tags.AddString("three")
	testVal.Tags.AddString("four")

	result, errset := bucket().Set(testVal).Run(con())

	if errset != nil {
		t.Error("Set 1: ", errset)
	}

	// Get it back
	var resVal ourTestType
	_, errget := bucket().Get(result.Key, &resVal).Run(con())

	if errget != nil {
		t.Error("Get: ", errget)
	}

	expected := []string{"one", "two", "three", "four"}
	sort.Strings(expected)

	val := resVal.Tags.Strings()
	sort.Strings(val)

	if !reflect.DeepEqual(expected, val) {
		t.Logf("Expected: %+v\n", expected)
		t.Logf("Got: %+v\n", val)
		t.Error("Unexpected value 1")
	}

	// Remove from fetched
	resVal.Tags.RemoveString("two")

	if errset != nil {
		t.Error("Set 2: ", resVal)
	}

	_, errset = bucket().Key(result.Key).Set(resVal).Run(con())

	if errset != nil {
		t.Error("Set 2: ", errset)
	}

	// Get it back
	var resVal2 ourTestType
	_, errget = bucket().Get(result.Key, &resVal2).Run(con())

	if errget != nil {
		t.Error("Get 2: ", errget)
	}

	expected = []string{"one", "three", "four"}
	sort.Strings(expected)

	val2 := resVal2.Tags.Strings()
	sort.Strings(val2)

	if !reflect.DeepEqual(expected, val2) {
		t.Logf("Expected: %+v\n", expected)
		t.Logf("Got: %+v\n", val2)
		t.Error("Unexpected value 2")
	}

}

func ExampleSet() {

	session, _ := Connect(ConnectOpts{
		Address: "127.0.0.1",
	})

	type Article struct {
		Tags *Set

		Context []byte `goriak:"goriakcontext"`
	}

	// Initializing a new Article and the Set within
	art := Article{
		Tags: NewSet(),
	}

	riakKey := "article-1"

	// Adding the tags "one" and "two"
	art.Tags.AddString("one")
	art.Tags.AddString("two")

	_, err := Bucket("bucket", "bucketType").Key(riakKey).Set(art).Run(session)

	if err != nil {
		// ..
	}

	// Retreiving from Riak
	var getArt Article
	_, err = Bucket("bucket", "bucketType").Get(riakKey, &getArt).Run(session)

	if err != nil {
		// ..
	}

	// Adding one extra tag.
	// Multiple AddString() and RemoveString() can be chained together before calling Exec().
	err = getArt.Tags.AddString("three").Exec(session)

	if err != nil {
		// ..
	}
}

func TestSetHas(t *testing.T) {
	s := NewSet()
	s.AddString("hello")
	s.AddString("it's")
	s.AddString("me")

	if !s.HasString("hello") {
		t.Error("Did not have hello")
	}

	if !s.HasString("it's") {
		t.Error("Did not have it's")
	}

	if s.HasString("i") {
		t.Error("Had i")
	}
}
