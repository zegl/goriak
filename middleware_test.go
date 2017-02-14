package goriak

import (
	"errors"
	"testing"
)

func TestMiddlewareSetAbort(t *testing.T) {
	m := func(cmd ExecMiddlewarer, next func() (*Result, error)) (*Result, error) {
		return nil, errors.New("aborted middleware")
	}

	_, err := Bucket("middleware", "tests").
		SetRaw([]byte{1, 2, 3, 4, 5}).
		AddMiddleware(m).
		Run(con())
	if err == nil {
		t.Error("no error")
	}

	if err.Error() != "aborted middleware" {
		t.Error("unexpected error")
	}
}

func TestMiddlewareSetKeyTest(t *testing.T) {
	beforeEmpty := 0
	afterWithKey := 0

	m := func(cmd ExecMiddlewarer, next func() (*Result, error)) (*Result, error) {
		if cmd.Key() == "" {
			beforeEmpty++
		}

		res, err := next()

		if cmd.Key() != "" {
			afterWithKey++
		}

		return res, err
	}

	_, err := Bucket("middleware", "tests").
		SetRaw([]byte{1, 2, 3, 4, 5}).
		AddMiddleware(m).
		AddMiddleware(m).
		Run(con())
	if err != nil {
		t.Error(err)
	}

	if beforeEmpty != 2 {
		t.Error("wrong beforeEmpty count")
	}

	if afterWithKey != 2 {
		t.Error("wrong afterWithKey count")
	}
}

func TestMiddlewareSetBucketType(t *testing.T) {
	m := func(cmd ExecMiddlewarer, next func() (*Result, error)) (*Result, error) {
		if cmd.Bucket() != "middleware" {
			t.Error("unexpected before bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected before bucket type")
		}

		res, err := next()

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected after bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected after bucket type")
		}

		return res, err
	}

	_, err := Bucket("middleware", "tests").
		SetRaw([]byte{1, 2, 3, 4, 5}).
		AddMiddleware(m).
		Run(con())
	if err != nil {
		t.Error(err)
	}
}
