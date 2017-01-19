package goriak

import (
	"reflect"
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

	result, err := bucket().Set(o).Run(con())

	if err != nil {
		t.Error("Set:", err)
	}

	var res objWithCustomType
	result2, err := bucket().Get(result.Key, &res).Run(con())

	if err != nil {
		t.Error("Get:", err)
	}

	if result2.NotFound {
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

type objWithByteSlice struct {
	ID  []byte
	Val string
}

func TestAutoMapByteSlice(t *testing.T) {
	id := []byte{4, 1, 3, 7, 8, 8, 4}

	o := objWithByteSlice{
		ID:  id,
		Val: "Byte Slice",
	}

	result, err := bucket().Set(o).Run(con())

	if err != nil {
		t.Error("Set", err)
	}

	var res objWithByteSlice
	result2, err := bucket().Get(result.Key, &res).Run(con())

	if err != nil {
		t.Error("Get", err)
	}

	if result2.NotFound {
		t.Error("not found")
	}

	if len(res.ID) != len(id) {
		t.Error("Did not get the same value of ID")
		t.Error(res.ID)
		return
	}

	for i := 0; i < len(id); i++ {
		if res.ID[i] != id[i] {
			t.Error("Did not get the same value of ID")
			t.Error(res.ID)
			return
		}
	}

	if res.Val != "Byte Slice" {
		t.Error("Wrong Val")
	}
}

func TestCustomTypeUint8(t *testing.T) {
	type CustomNumber uint8
	type CustomString string
	type CustomSlice []uint8
	type tt struct {
		Num CustomNumber
		Str CustomString
		Sli CustomSlice
	}

	input := tt{
		Num: 50,
		Str: "Hello",
		Sli: CustomSlice{1, 2, 3, 4, 5},
	}

	res, err := bucket().Set(input).Run(con())
	if err != nil {
		t.Error(err)
	}

	var output tt

	_, err = bucket().Get(res.Key, &output).Run(con())
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(input, output) {
		t.Logf("%+v", input)
		t.Logf("%+v", output)
	}

	t.Logf("%+v", output)
}
