# goriak ![Build Status](https://api.travis-ci.org/zegl/goriak.svg)

Everything is currenly in alfa status, stay tuned. :)

# What is goriak?

goriak is a wrapper around [riak-go-client](https://github.com/basho/riak-go-client) to make it easier and more friendly for developers to user Riak.

# Installation

```bash
go get -u github.com/zegl/goriak
```

# Maps

goriak can automatically create [Riak maps](http://docs.basho.com/riak/kv/2.1.4/developing/data-types/) from your Go types.

## SetMap

In the example below `Name` will be saved as a register, and `Aliases` will be a set.

```go
type User struct {
    Name    string
    Aliases []string
}

user := User {
    Name:   "Foo",
    Alises: []string{"Foo", "Bar"},
}

goriak.SetMap("bucket-name", "bucket-type", "key", user)
```

## GetMap

The map can later be retreived as a whole:

```go
var res User
goriak.GetMap("bucket-name", "bucket-type", "key", &res)
```

## MapOperation

There are ofcourse times where you want to perform Riak Map Operations.

```go
// Works with MapOperation from github.com/basho/riak-go-client
operation := goriak.NewMapOperation()
operation.AddToSet("Aliases", []byte("Baz"))

goriak.MapOperation("bucket-name", "bucket-type", "key", operation)
```

# Values

Values are automatically JSON encoded and decoded.

## SetValue

```go
goriak.SetValue("bucket-name", "bucket-type", "key", obj)
```

## GetValue

```go
goriak.GetValue("bucket-name", "bucket-type", "key", &obj)
```

# Secondary Indexes

You can set secondary indexes on Values with `SetValue` by using struct tags.

```go
type User struct {
    Name    string `goriakindex:"nameindex_bin"`
    Aliases []string
}
```

When saved the next time the index will be updated.

Keys in a particular index can be retreived with `KeysInIndex`.

```go
goriak.KeysInIndex("bucket-name", "bucket-type", "nameindex_bin", "Value")
```

Indexes can also be used in slices. If you are using a slice every value in the slice will be added to the index.

```go
type User struct {
    Aliases []string `goriakindex:"aliasesindex_bin"`
}
```