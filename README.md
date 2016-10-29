# goriak [![Build Status](https://circleci.com/gh/zegl/goriak.svg?style=svg)](https://circleci.com/gh/zegl/goriak) [![codecov](https://codecov.io/gh/zegl/goriak/branch/master/graph/badge.svg)](https://codecov.io/gh/zegl/goriak)


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

There is a time in everyones life where you need to perform raw MapOperations.

Some operations, such as `RemoveFromSet` requires a Context to perform the operation.
A Context can be retreived from `GetMap` by setting a special context type.


```go
type ourType struct {
    Aliases []string

    // The context from Riak will be added if the tag goriakcontext is provided
    Context []byte `goriak:"goriakcontext"`
}

// ... GetMap()

// Works with MapOperation from github.com/basho/riak-go-client
operation := goriak.NewMapOperation()
operation.AddToSet("Aliases", []byte("Baz"))

goriak.MapOperation("bucket-name", "bucket-type", "key", operation, val.Context)
```

## Supported Go types


|  Go Type   | Riak Type |
|------------|-----------|
| `struct`   | map       |
| `string`   | register  |
| `[n]byte`  | register  |
| `[]byte`   | register  |
| `[]slice`  | set       |
| `[]slice`  | set       |
| `[][]byte` | set       |
| `map`      | map       |


### Golang map types

Supported key types: `int`, `int8`, `int16`, `int32`, `int64`, `string`.  
Supported value types: `string`, `[]byte`.

## Counters

Riak Counters is supported with the special `goriak.Counter` type.

Example:

```go
type Article struct {
    Title string
    Views *goriak.Counter
}

// Get our object
var article Article
con.GetMap("articles", "map", "1-hello-world", &article)

// Increase views by 1
err := article.Views.Increase(1).Exec(con)

// check err
```

`Counter.Exec(con)` will make a lightweight request to Riak, and the counter is the only object that will be updated.

You can also save the changes to your counter with `SetMap()`, this is useful if you want to change multiple counters at the same time.

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
