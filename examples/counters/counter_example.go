package main

import (
	"github.com/zegl/goriak"

	"fmt"
)

type User struct {
	ID     int
	Name   string
	Visits *goriak.Counter
}

func main() {
	con, err := goriak.Connect(goriak.ConnectOpts{Address: "127.0.0.1"})

	if err != nil {
		panic(err)
	}

	user := User{
		ID:   400,
		Name: "FooBar",
	}

	// Save our User object to Riak
	_, err = goriak.Bucket("bucket", "bucketType").
		Key("user-400").
		Set(user).
		Run(con)

	if err != nil {
		panic(err)
	}

	// Retreive the same object from Riak
	var getUser User

	resp, err := goriak.Bucket("bucket", "bucketType").
		Get("user-400", &getUser).
		Run(con)

	if err != nil {
		panic(err)
	}

	if resp.NotFound {
		panic("Not found")
	}

	fmt.Printf("%+v", getUser)

	err = getUser.Visits.Increase(1).Exec(con)

	if err != nil {
		panic(err)
	}

	fmt.Println("Visits: ", getUser.Visits.Value())
}
