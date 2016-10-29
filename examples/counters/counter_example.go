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
	r, err := goriak.NewGoriak("127.0.0.1")

	if err != nil {
		panic(err)
	}

	user := User{
		ID:   400,
		Name: "FooBar",
	}

	// Save our User object to Riak
	err = r.SetMap("bucket", "bucketType", "user-400", user)

	if err != nil {
		panic(err)
	}

	// Retreive the same object from Riak
	var getUser User
	err, isNotFound := r.GetMap("bucket", "bucketType", "user-400", &getUser)

	if err != nil {
		panic(err)
	}

	if isNotFound {
		panic("Not found")
	}

	fmt.Printf("%+v", getUser)

	err = getUser.Visits.Increase(1).Exec(r)

	if err != nil {
		panic(err)
	}

	fmt.Println("Visits: ", getUser.Visits.Value())
}
