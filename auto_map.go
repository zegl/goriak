package goriak

import (
	"fmt"
	"reflect"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

func Set(bucket, key string, input interface{}) {
	op := riak.MapOperation{}

	rValue := reflect.ValueOf(input).Elem()
	rType := reflect.TypeOf(input).Elem()
	num := rType.NumField()

	for i := 0; i < num; i++ {
		field := rType.Field(i)

		itemKey := field.Name

		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			itemKey = tag
		}

		// Register: String
		if field.Type.Kind() == reflect.String {
			op.SetRegister(itemKey, []byte(rValue.Field(i).String()))
			continue
		}

		// Set
		if field.Type.Kind() == reflect.Slice {

			sliceLength := rValue.Field(i).Len()
			sliceVal := rValue.Field(i).Slice(0, sliceLength)

			// Slice: Int
			if rType.Field(i).Type.Elem().Kind() == reflect.Int {

				// Convert Int -> String -> []byte
				for ii := 0; ii < sliceLength; ii++ {
					intVal := sliceVal.Index(ii).Int()
					strVal := strconv.FormatInt(intVal, 10)
					op.AddToSet(itemKey, []byte(strVal))
				}

				continue
			}

			fmt.Println("Unknown Slice:", sliceVal.Index(0).Type())
			fmt.Printf("%+v\n", sliceVal)
			continue
		}

		fmt.Println("Unknown type")
		fmt.Println(field.Type.Kind())
		fmt.Printf("%+v\n\n", field)

	}

	fmt.Printf("%+v\n\n", op)

	cmd, err := riak.NewUpdateMapCommandBuilder().WithBucket(bucket).WithKey(key).WithBucketType("maps").WithMapOperation(&op).Build()

	if err != nil {
		panic(err)
	}

	err = connect().Execute(cmd)

	if err != nil {
		panic(err)
	}
}

func Get(bucket, key string, output interface{}) {
	cmd, err := riak.NewFetchMapCommandBuilder().
		WithBucket(bucket).
		WithKey(key).
		WithBucketType("maps").
		Build()

	if err != nil {
		panic(err)
	}

	err = connect().Execute(cmd)

	if err != nil {
		panic(err)
	}

	ma := cmd.(*riak.FetchMapCommand)

	data := ma.Response.Map

	// Set values
	rValue := reflect.ValueOf(output).Elem()
	rType := reflect.TypeOf(output).Elem()
	num := rType.NumField()

	for i := 0; i < num; i++ {

		field := rType.Field(i)
		registerName := field.Name
		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			registerName = tag
		}

		// String
		if field.Type.Kind() == reflect.String {
			if val, ok := data.Registers[registerName]; ok {
				rValue.Field(i).SetString(string(val))
			}

			continue
		}

		// Slice
		if field.Type.Kind() == reflect.Slice {
			if rValue.Field(i).Type().Elem().Kind() == reflect.Int {

				// Slice : Int
				if setVal, ok := data.Sets[registerName]; ok {
					result := make([]int, len(setVal))

					for i, v := range setVal {
						intVal, err := strconv.ParseInt(string(v), 10, 64)

						if err != nil {
							panic(err)
						}

						result[i] = int(intVal)
					}

					rValue.Field(i).Set(reflect.ValueOf(result))
					continue
				}
			}
		}

		fmt.Println("Unknown type:", field.Type.Kind().String())
	}
}
