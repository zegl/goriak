package goriak

import (
	"reflect"
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
