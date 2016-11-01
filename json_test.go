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
