# goriak [![Build Status](https://circleci.com/gh/zegl/goriak.svg?style=svg)](https://circleci.com/gh/zegl/goriak) [![codecov](https://codecov.io/gh/zegl/goriak/branch/v2/graph/badge.svg)](https://codecov.io/gh/zegl/goriak/branch/v2) [![Go Report Card](https://goreportcard.com/badge/gopkg.in/zegl/goriak.v2)](https://goreportcard.com/report/gopkg.in/zegl/goriak.v2)

[![Join the chat at https://gitter.im/golangriak/Lobby](https://badges.gitter.im/golangriak/Lobby.svg)](https://gitter.im/golangriak/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Current version: `v2.1.0`.  
Riak KV version: 2.0 or higher, the latest version of Riak KV is always recommended. 

# What is goriak?

goriak is a wrapper around [riak-go-client](https://github.com/basho/riak-go-client) to make it easier and more friendly for developers to use Riak KV.

# Installation

```bash
go get -u gopkg.in/zegl/goriak.v2
```

# Maps (Riak Data Types)

The main feature of goriak is that goriak automatically can marshal/unmarshal your Go types into [Riak data types](http://docs.basho.com/riak/kv/2.1.4/developing/data-types/).

## Set (Riak Data Types)

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

goriak.Bucket("bucket-name", "bucket-type").Key("key").Set(user).Run(c)
```

## Get (Riak Data Types)

The map can later be retreived as a whole:

```go
var res User
goriak.Bucket("bucket-name", "bucket-type").Key("key").Get(&res).Run(c)
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
| `int` [1]  | register  |

1: All signed integer types are supported.

### Golang map types

Supported key types: `int`, `int8`, `int16`, `int32`, `int64`, `string`.  
Supported value types: `string`, `[]byte`.

## Helper types

Some actions are more complicated then necessary with the use of the default Go types and `MapOperations`.

This is why goriak contains the types `goriak.Counter` and `goriak.Set`. Both of these types will help you performing actions such as incrementing a value, or adding/removing items.

### Counters

Riak Counters is supported with the special `goriak.Counter` type.

Example:

```go
type Article struct {
    Title string
    Views *goriak.Counter
}

// Get our object
var article Article
goriak.Bucket("articles", "map").Key("1-hello-world").Get(&article).Run(con)

// Increase views by 1
err := article.Views.Increase(1).Exec(con)

// check err
```

`Counter.Exec(con)` will make a lightweight request to Riak, and the counter is the only object that will be updated.

You can also save the changes to your counter with `SetMap()`, this is useful if you want to change multiple counters at the same time.

Check [godoc](https://godoc.org/github.com/zegl/goriak) for more information.

### Sets

You can chose to use `goriak.Set` to help you with Set related actions, such as adding and removing items. `goriak.Set` also has support for sending incremental actions to Riak so that you don't have to build that functionality yourself.

Example:

```go
type Article struct {
    Title string
    Tags *goriak.Set
}

// Get our object
var article Article
goriak.Bucket("articles", "map").Key("1-hello-world").Get(&article).Run(con)

// Add the tag "animals"
err := article.Tags.AddString("animals").Exec(con)

// check err
```

Check [godoc](https://godoc.org/github.com/zegl/goriak#Set) for more information.

# Values

Values can be automatically JSON Marshalled/Unmarshalled by using `SetJSON()` and `GetJSON()`.
There is also `SetRaw()` and `GetRaw()` that works directly on `[]byte`s.

## SetJSON

```go
goriak.Bucket("bucket-name", "bucket-type").Key("key").SetJSON(obj).Run(con)
```

## GetJSON

```go
goriak.Bucket("bucket-name", "bucket-type").Key("key").GetJSON(&obj).Run(con)
```

## MapOperation

There is a time in everyones life where you need to perform raw MapOperations on your Riak Data Values.

Some operations, such as `RemoveFromSet` requires a Context to perform the operation.
A Context can be retreived from `Get` by setting a special context type.


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


# Secondary Indexes

You can set secondary indexes automatically with `SetJSON()` by using struct tags.

```go
type User struct {
    Name    string `goriakindex:"nameindex_bin"`
    Aliases []string
}
```

Indexes can also be used in slices. If you are using a slice every value in the slice will be added to the index.

```go
type User struct {
    Aliases []string `goriakindex:"aliasesindex_bin"`
}
```


When saved the next time the index will be updated.

## KeysInIndex

Keys in a particular index can be retreived with `KeysInIndex`.

```go
callback := func(item goriak.SecondaryIndexQueryResult) {
    // use item
}

goriak.Bucket("bucket-name", "bucket-type").
    KeysInIndex("nameindex_bin", "Value", callback).
    Run(con)
```

## AddToIndex

An alternative way of setting Secondary Indexes is by using `AddToIndex()`.

```go
goriak.Bucket("bucket-name", "bucket-type").
    AddToIndex("indexname_bin", "value").
    SetRaw(data).
    Run(con)
```