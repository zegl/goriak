package goriak

import (
	"testing"
)

func TestSetWithConsistancyOptions(t *testing.T) {
	_, err := Bucket("raw", "default").
		SetRaw([]byte("Hello Raw")).
		WithPw(4).Run(con())

	expectedError := "ClientError|[Cluster] all retries exhausted and/or no nodes available to execute command|InnerError|RiakError|0|{n_val_violation,3}"

	if err.Error() != expectedError {
		t.Error("Unexpected error PW:", err)
	}

	_, err = Bucket("raw", "default").
		SetRaw([]byte("Hello Raw")).
		WithDw(4).Run(con())

	if err.Error() != expectedError {
		t.Error("Unexpected error DW:", err)
	}

	_, err = Bucket("raw", "default").
		SetRaw([]byte("Hello Raw")).
		WithW(4).Run(con())

	if err.Error() != expectedError {
		t.Error("Unexpected error W:", err)
	}
}

func TestGetWithConsistancyOptions(t *testing.T) {
	input, err := Bucket("raw", "default").
		SetRaw([]byte("Hello Raw")).
		Run(con())

	_, err = Bucket("raw", "default").
		GetRaw(input.Key, &[]byte{}).
		WithPr(4).Run(con())

	expectedError := "ClientError|[Cluster] all retries exhausted and/or no nodes available to execute command|InnerError|RiakError|0|{n_val_violation,3}"

	if err.Error() != expectedError {
		t.Error("Unexpected error PW:", err)
	}

	_, err = Bucket("raw", "default").
		GetRaw(input.Key, &[]byte{}).
		WithR(4).Run(con())

	if err.Error() != expectedError {
		t.Error("Unexpected error PW:", err)
	}
}
