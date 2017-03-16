package riak

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

var localhost = net.ParseIP("127.0.0.1")

var vclock = bytes.NewBufferString("vclock123456789")
var vclockBytes = vclock.Bytes()

var crdtContext = bytes.NewBufferString("crdt_context")
var crdtContextBytes = crdtContext.Bytes()

const testBucketType = "plain"
const testBucketName = "riak_index_tests"

// riak_admin bucket-type create counters '{"props":{"datatype":"counter"}}'
// riak-admin bucket-type activate counters
const testCounterBucketType = "counters"

// riak_admin bucket-type create sets '{"props":{"datatype":"set"}}'
// riak-admin bucket-type activate sets
const testSetBucketType = "sets"

// riak_admin bucket-type create maps '{"props":{"datatype":"map"}}'
// riak-admin bucket-type activate maps
const testMapBucketType = "maps"

// riak_admin bucket-type create hlls '{"props":{"datatype":"hll"}}'
// riak-admin bucket-type activate hlls
const testHllBucketType = "hlls"

func getRiakPort() uint16 {
	riakPort := uint16(8087)
	if portEnvVar := os.Getenv("RIAK_PORT"); portEnvVar != "" {
		if portNum, err := strconv.Atoi(portEnvVar); err == nil {
			riakPort = uint16(portNum)
		}
	}
	return riakPort
}

func getRiakHost() string {
	riakHost := "localhost"
	if hostEnvVar := os.Getenv("RIAK_HOST"); hostEnvVar != "" {
		riakHost = hostEnvVar
	}
	return riakHost
}

func getRiakAddress() string {
	return fmt.Sprintf("%s:%d", getRiakHost(), getRiakPort())
}

func getBasicObject() *Object {
	return &Object{
		ContentType:     "text/plain",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte("this is a value in Riak"),
	}
}

func TestDeleteFromSliceWhileIterating(t *testing.T) {
	s := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(s) != 10 {
		t.Errorf("expected 10 elements, got %v", len(s))
	}
	for i := 0; i < len(s); {
		e := s[i]
		// t.Log(i, "Processing:", e)
		if e%2 == 0 {
			l := len(s) - 1
			s[i], s[l], s = s[l], 0, s[:l]
		} else {
			i++
		}
	}
	if len(s) != 5 {
		t.Errorf("expected 5 elements, got %v", len(s))
	}
}

func jsonDump(val interface{}) {
	EnableDebugLogging = true
	defer func() {
		EnableDebugLogging = false
	}()
	if val == nil {
		logDebug("[jsonDump]", "NIL VAL")
	} else {
		if json, err := json.MarshalIndent(val, "", "  "); err != nil {
			logDebug("[jsonDump]", "%s", err.Error())
		} else {
			logDebug("[jsonDump]", "%s", string(json))
		}
	}
}

func validateTimeout(t *testing.T, e time.Duration, a uint32) {
	actualDuration := time.Duration(a) * time.Millisecond
	if expected, actual := e, actualDuration; expected != actual {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func sliceIncludes(slice [][]byte, term []byte) (rv bool) {
	rv = false
	for _, t := range slice {
		if bytes.Compare(t, term) == 0 {
			rv = true
			break
		}
	}
	return
}

var randomBytes = bytes.NewBufferString(random).Bytes()

var random = "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"
