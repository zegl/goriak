// +build integration

package riak

import (
	"fmt"
	"testing"
	"time"
)

// UpdateCounter

func TestUpdateAndFetchCounter(t *testing.T) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	var err error
	var cmd Command

	b1 := NewUpdateCounterCommandBuilder()
	cmd, err = b1.WithBucketType(testCounterBucketType).
		WithBucket(testBucketName).
		WithReturnBody(true).
		WithIncrement(10).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	key := "unknown"
	if uc, ok := cmd.(*UpdateCounterCommand); ok {
		if got, want := uc.isLegacy, false; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if rsp.GeneratedKey == "" {
			t.Errorf("expected non-empty generated key")
		} else {
			key = rsp.GeneratedKey
			if expected, actual := int64(10), rsp.CounterValue; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.FailNow()
	}

	b2 := NewFetchCounterCommandBuilder()
	cmd, err = b2.WithBucketType(testCounterBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchCounterCommand); ok {
		if expected, actual := int64(10), fc.Response.CounterValue; expected != actual {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	} else {
		t.FailNow()
	}
}

// UpdateSet

func TestUpdateAndFetchSet(t *testing.T) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	var err error
	var cmd Command

	adds := [][]byte{
		[]byte("a1"),
		[]byte("a2"),
		[]byte("a3"),
		[]byte("a4"),
	}

	b1 := NewUpdateSetCommandBuilder()
	cmd, err = b1.WithBucketType(testSetBucketType).
		WithBucket(testBucketName).
		WithReturnBody(true).
		WithAdditions(adds...).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	key := "unknown"
	if uc, ok := cmd.(*UpdateSetCommand); ok {
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if rsp.GeneratedKey == "" {
			t.Errorf("expected non-empty generated key")
		} else {
			key = rsp.GeneratedKey
			for i := 1; i <= 4; i++ {
				sitem := fmt.Sprintf("a%d", i)
				if expected, actual := true, sliceIncludes(rsp.SetValue, []byte(sitem)); expected != actual {
					t.Errorf("expected %v, got %v", expected, actual)
				}
			}
		}
	} else {
		t.FailNow()
	}

	b2 := NewFetchSetCommandBuilder()
	cmd, err = b2.WithBucketType(testSetBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchSetCommand); ok {
		if fc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := fc.Response
		for i := 1; i <= 4; i++ {
			sitem := fmt.Sprintf("a%d", i)
			if expected, actual := true, sliceIncludes(rsp.SetValue, []byte(sitem)); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
	} else {
		t.FailNow()
	}
}

// UpdateMap

func TestUpdateAndFetchMap(t *testing.T) {
	cluster := integrationTestsBuildCluster()
	defer func() {
		if err := cluster.Stop(); err != nil {
			t.Error(err.Error())
		}
	}()

	var err error
	var cmd Command

	mapOp := &MapOperation{}
	mapOp.IncrementCounter("counter_1", 50).
		AddToSet("set_1", []byte("value_1")).
		SetRegister("register_1", []byte("register_value_1")).
		SetFlag("flag_1", true)
	mapOp.Map("map_2").IncrementCounter("counter_1", 50).
		AddToSet("set_1", []byte("value_1")).
		SetRegister("register_1", []byte("register_value_1")).
		SetFlag("flag_1", true).
		Map("map_3")
	b1 := NewUpdateMapCommandBuilder()
	cmd, err = b1.WithBucketType(testMapBucketType).
		WithBucket(testBucketName).
		WithMapOperation(mapOp).
		WithReturnBody(true).
		WithTimeout(time.Second * 20).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	key := "unknown"
	if uc, ok := cmd.(*UpdateMapCommand); ok {
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if rsp.GeneratedKey == "" {
			t.Errorf("expected non-empty generated key")
		} else {
			key = rsp.GeneratedKey
		}
	} else {
		t.FailNow()
	}

	var context []byte
	b2 := NewFetchMapCommandBuilder()
	cmd, err = b2.WithBucketType(testMapBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if fc, ok := cmd.(*FetchMapCommand); ok {
		if fc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := fc.Response
		var vmap = func(m *Map) {
			if expected, actual := int64(50), m.Counters["counter_1"]; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "value_1", string(m.Sets["set_1"][0]); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := "register_value_1", string(m.Registers["register_1"]); expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
			if expected, actual := true, m.Flags["flag_1"]; expected != actual {
				t.Errorf("expected %v, got %v", expected, actual)
			}
		}
		vmap(rsp.Map)
		vmap(rsp.Map.Maps["map_2"])
		context = rsp.Context
	} else {
		t.FailNow()
	}

	mapOp = &MapOperation{}
	mapOp.RemoveCounter("counter_1")
	b3 := NewUpdateMapCommandBuilder()
	cmd, err = b3.WithBucketType(testMapBucketType).
		WithBucket(testBucketName).
		WithKey(key).
		WithMapOperation(mapOp).
		WithContext(context).
		WithReturnBody(true).
		WithTimeout(time.Second * 20).
		Build()
	if err != nil {
		t.Fatal(err.Error())
	}
	if err = cluster.Execute(cmd); err != nil {
		t.Fatal(err.Error())
	}
	if uc, ok := cmd.(*UpdateMapCommand); ok {
		if uc.Response == nil {
			t.Fatal("expected non-nil Response")
		}
		rsp := uc.Response
		if _, ok := rsp.Map.Counters["counter_1"]; ok {
			t.Error("counter_1 should have been removed")
		}
	} else {
		t.FailNow()
	}
}
