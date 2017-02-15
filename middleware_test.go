package goriak

import (
	"errors"
	"testing"
)

func TestMiddlewareSetAbort(t *testing.T) {
	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
		return nil, errors.New("aborted middleware")
	}

	_, err := Bucket("middleware", "tests").
		RegisterRunMiddleware(m).
		SetRaw([]byte{1, 2, 3, 4, 5}).
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

	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
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
		RegisterRunMiddleware(m).
		RegisterRunMiddleware(m).
		SetRaw([]byte{1, 2, 3, 4, 5}).
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
	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
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
		RegisterRunMiddleware(m).
		SetRaw([]byte{1, 2, 3, 4, 5}).
		Run(con())
	if err != nil {
		t.Error(err)
	}
}
func TestMiddlewareSetJSON(t *testing.T) {
	exec := false

	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
		exec = true

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
		RegisterRunMiddleware(m).
		SetJSON([]byte{1, 2, 3, 4, 5}).
		Run(con())
	if err != nil {
		t.Error(err)
	}

	if !exec {
		t.Error("middleware did not run")
	}
}

func TestMiddlewareGetRaw(t *testing.T) {
	exec := false

	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
		exec = true

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected before bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected before bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected before key")
		}

		res, err := next()

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected after bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected after bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected after key")
		}

		if err == nil {
			t.Error("did not get error")
		}

		return res, err
	}

	var out []byte
	Bucket("middleware", "tests").
		RegisterRunMiddleware(m).
		GetRaw("hello123", &out).
		Run(con())

	if !exec {
		t.Error("middleware did not execute")
	}
}

func TestMiddlewareGetJSON(t *testing.T) {
	exec := false

	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
		exec = true

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected before bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected before bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected before key")
		}

		res, err := next()

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected after bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected after bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected after key")
		}

		if err == nil {
			t.Error("did not get error")
		}

		return res, err
	}

	var out string

	Bucket("middleware", "tests").
		RegisterRunMiddleware(m).
		GetJSON("hello123", &out).
		Run(con())

	if !exec {
		t.Error("middleware did not execute")
	}
}

func TestMiddlewareGet(t *testing.T) {
	exec := false

	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
		exec = true

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected before bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected before bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected before key")
		}

		res, err := next()

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected after bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected after bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected after key")
		}

		if err == nil {
			t.Error("did not get error")
		}

		return res, err
	}

	var out string

	Bucket("middleware", "tests").
		RegisterRunMiddleware(m).
		Get("hello123", &out).
		Run(con())

	if !exec {
		t.Error("middleware did not execute")
	}
}

func TestMiddlewareSet(t *testing.T) {
	exec := false

	m := func(cmd RunMiddlewarer, next func() (*Result, error)) (*Result, error) {
		exec = true

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected before bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected before bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected before key")
		}

		res, err := next()

		if cmd.Bucket() != "middleware" {
			t.Error("unexpected after bucket")
		}
		if cmd.BucketType() != "tests" {
			t.Error("unexpected after bucket type")
		}
		if cmd.Key() != "hello123" {
			t.Error("unexpected after key")
		}

		if err == nil {
			t.Error("did not get error")
		}

		return res, err
	}

	var out string

	Bucket("middleware", "tests").
		RegisterRunMiddleware(m).
		Set(out).
		Key("hello123").
		Run(con())

	if !exec {
		t.Error("middleware did not execute")
	}
}
