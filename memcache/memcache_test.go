/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const testServer = "localhost:11211"

func setup(t *testing.T) bool {
	c, err := net.Dial("tcp", testServer)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", testServer)
	}
	c.Write([]byte("flush_all\r\n"))
	c.Close()
	return true
}

func TestLocalhost(t *testing.T) {
	if !setup(t) {
		return
	}
	testWithClient(t, New(testServer))
}

// Run the memcached binary as a child process and connect to its unix socket.
func TestUnixSocket(t *testing.T) {
	sock := fmt.Sprintf("/tmp/test-gomemcache-%d.sock", os.Getpid())
	cmd := exec.Command("memcached", "-s", sock)
	if err := cmd.Start(); err != nil {
		t.Skipf("skipping test; couldn't find memcached")
		return
	}
	defer cmd.Wait()
	defer cmd.Process.Kill()

	// Wait a bit for the socket to appear.
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(time.Duration(25*i) * time.Millisecond)
	}

	testWithClient(t, New(sock))
}

func mustSetF(t *testing.T, c *Client) func(*Item) {
	return func(it *Item) {
		if err := c.Set(it); err != nil {
			t.Fatalf("failed to Set %#v: %v", *it, err)
		}
	}
}

func testWithClient(t *testing.T, c *Client) {
	checkErr := func(err error, format string, args ...interface{}) {
		if err != nil {
			t.Fatalf(format, args...)
		}
	}
	mustSet := mustSetF(t, c)

	// Set
	foo := &Item{Key: "foo", Value: []byte("fooval"), Flags: 123}
	err := c.Set(foo)
	checkErr(err, "first set(foo): %v", err)
	err = c.Set(foo)
	checkErr(err, "second set(foo): %v", err)

	// Get
	it, err := c.Get("foo")
	checkErr(err, "get(foo): %v", err)
	if it.Key != "foo" {
		t.Errorf("get(foo) Key = %q, want foo", it.Key)
	}
	if string(it.Value) != "fooval" {
		t.Errorf("get(foo) Value = %q, want fooval", string(it.Value))
	}
	if it.Flags != 123 {
		t.Errorf("get(foo) Flags = %v, want 123", it.Flags)
	}

	// Get and set a unicode key
	quxKey := "Hello_世界"
	qux := &Item{Key: quxKey, Value: []byte("hello world")}
	err = c.Set(qux)
	checkErr(err, "first set(Hello_世界): %v", err)
	it, err = c.Get(quxKey)
	checkErr(err, "get(Hello_世界): %v", err)
	if it.Key != quxKey {
		t.Errorf("get(Hello_世界) Key = %q, want Hello_世界", it.Key)
	}
	if string(it.Value) != "hello world" {
		t.Errorf("get(Hello_世界) Value = %q, want hello world", string(it.Value))
	}

	// Set malformed keys
	malFormed := &Item{Key: "foo bar", Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo bar) should return ErrMalformedKey instead of %v", err)
	}
	malFormed = &Item{Key: "foo" + string(rune(0x7f)), Value: []byte("foobarval")}
	err = c.Set(malFormed)
	if err != ErrMalformedKey {
		t.Errorf("set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)
	}

	// Add
	bar := &Item{Key: "bar", Value: []byte("barval")}
	err = c.Add(bar)
	checkErr(err, "first add(foo): %v", err)
	if err := c.Add(bar); err != ErrNotStored {
		t.Fatalf("second add(foo) want ErrNotStored, got %v", err)
	}

	// Replace
	baz := &Item{Key: "baz", Value: []byte("bazvalue")}
	if err := c.Replace(baz); err != ErrNotStored {
		t.Fatalf("expected replace(baz) to return ErrNotStored, got %v", err)
	}
	err = c.Replace(bar)
	checkErr(err, "replaced(foo): %v", err)

	// GetMulti
	m, err := c.GetMulti([]string{"foo", "bar"})
	checkErr(err, "GetMulti: %v", err)
	if g, e := len(m), 2; g != e {
		t.Errorf("GetMulti: got len(map) = %d, want = %d", g, e)
	}
	if _, ok := m["foo"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'foo'")
	}
	if _, ok := m["bar"]; !ok {
		t.Fatalf("GetMulti: didn't get key 'bar'")
	}
	if g, e := string(m["foo"].Value), "fooval"; g != e {
		t.Errorf("GetMulti: foo: got %q, want %q", g, e)
	}
	if g, e := string(m["bar"].Value), "barval"; g != e {
		t.Errorf("GetMulti: bar: got %q, want %q", g, e)
	}

	// Delete
	err = c.Delete("foo")
	checkErr(err, "Delete: %v", err)
	it, err = c.Get("foo")
	if err != ErrCacheMiss {
		t.Errorf("post-Delete want ErrCacheMiss, got %v", err)
	}

	// Incr/Decr
	mustSet(&Item{Key: "num", Value: []byte("42")})
	n, err := c.Increment("num", 8)
	checkErr(err, "Increment num + 8: %v", err)
	if n != 50 {
		t.Fatalf("Increment num + 8: want=50, got=%d", n)
	}
	n, err = c.Decrement("num", 49)
	checkErr(err, "Decrement: %v", err)
	if n != 1 {
		t.Fatalf("Decrement 49: want=1, got=%d", n)
	}
	err = c.Delete("num")
	checkErr(err, "delete num: %v", err)
	n, err = c.Increment("num", 1)
	if err != ErrCacheMiss {
		t.Fatalf("increment post-delete: want ErrCacheMiss, got %v", err)
	}
	mustSet(&Item{Key: "num", Value: []byte("not-numeric")})
	n, err = c.Increment("num", 1)
	if err == nil || !strings.Contains(err.Error(), "client error") {
		t.Fatalf("increment non-number: want client error, got %v", err)
	}
	testTouchWithClient(t, c)

	// Test Delete All
	err = c.DeleteAll()
	checkErr(err, "DeleteAll: %v", err)
	it, err = c.Get("bar")
	if err != ErrCacheMiss {
		t.Errorf("post-DeleteAll want ErrCacheMiss, got %v", err)
	}

	// Test Ping
	err = c.Ping()
	checkErr(err, "error ping: %s", err)

	//test Meta get command
	testMetaGetWithClient(t, c, checkErr)
}

func testMetaGetWithClient(t *testing.T, c *Client,
	checkErr func(err error, format string, args ...interface{})) {
	c.DeleteAll()
	defer c.DeleteAll()

	//preparing some test data for test cases
	key := &Item{Key: "key", Value: []byte("value")}
	err := c.Set(key)
	checkErr(err, "first set(key): %v", err)

	key2 := &Item{Key: "key2", Value: []byte("value\r\n"), Flags: 345}
	err = c.Set(key2)
	checkErr(err, "second set(key): %v", err)

	key3 := &Item{Key: "key3", Value: []byte("value 3")}
	err = c.Set(key3)
	checkErr(err, "third set(key3): %v", err)

	//simple meta get
	respMetadata, err := c.MetaGet("key", nil)
	checkErr(err, "first metaGet(key): %v", err)
	if string(respMetadata.ReturnItemValue) != "value" {
		t.Errorf("metaGet(key) Actual Value=%q, Expected Value=value", string(respMetadata.ReturnItemValue))
	}

	respMetadata, err = c.MetaGet("key", &MetaGetFlags{})
	checkErr(err, "second metaGet(key): %v", err)
	if string(respMetadata.ReturnItemValue) != "value" {
		t.Errorf("metaGet(key) Actual Value=%q, Expected Value=value", string(respMetadata.ReturnItemValue))
	}

	//meta get with base64 key , returnItemHitInResponse, LastAccessedTime, ItemSize and flags
	respMetadata, err = c.MetaGet("a2V5Mg==", &MetaGetFlags{IsKeyBase64: true,
		ReturnItemHitInResponse: true, ReturnLastAccessedTimeSecondsInResponse: true,
		ReturnItemSizeBytesInResponse: true, ReturnClientFlagsInResponse: true,
		ReturnKeyInResponse: true})
	checkErr(err, "third metaGet(key2): %v", err)
	if string(respMetadata.ReturnItemValue) != "value\r\n" {
		t.Errorf("metaGet(key2) Value=%q, Expected Value=value", string(respMetadata.ReturnItemValue))
	}
	if respMetadata.isItemHitBefore == nil || *respMetadata.isItemHitBefore == true {
		t.Errorf("metaGet(key2) isItemHitBefore should not be nil but should be false")
	}
	if respMetadata.TimeInSecondsSinceLastAccessed == nil || *respMetadata.TimeInSecondsSinceLastAccessed != 0 {
		t.Errorf("metaGet(key2) TimeInSecondsSinceLastAccessed should not be nil but should be 0")
	}
	if respMetadata.ItemSizeInBytes == nil || *respMetadata.ItemSizeInBytes != 7 {
		t.Errorf("metaGet(key2) ItemSizeInBytes should not be nil but should be 7")
	}
	if respMetadata.ClientFlag == nil || *respMetadata.ClientFlag != 345 {
		t.Errorf("metaGet(key2) ClientFlag should not be nil but should be 345")
	}
	if respMetadata.ItemKey == nil || *respMetadata.ItemKey != "key2" {
		t.Errorf("metaGet(key2) ItemKey should not be nil but should be key2")
	}

	//sleep so that we can test ReturnLastAccessedTimeInSeconds
	time.Sleep(2 * time.Second)

	respMetadata, err = c.MetaGet("key", &MetaGetFlags{
		ReturnItemHitInResponse: true, ReturnLastAccessedTimeSecondsInResponse: true})
	checkErr(err, "metaGet(key3): %v", err)
	if respMetadata.isItemHitBefore == nil || *respMetadata.isItemHitBefore == false {
		t.Errorf("metaGet(key3) isItemHitBefore should not be nil but should be true")
	}
	if respMetadata.TimeInSecondsSinceLastAccessed == nil || *respMetadata.TimeInSecondsSinceLastAccessed == 0 {
		t.Errorf("metaGet(key2) TimeInSecondsSinceLastAccessed should not be nil but should be non zero")
	}

	//meta get cache miss
	respMetadata, err = c.MetaGet("key53", &MetaGetFlags{})
	if err != ErrCacheMiss {
		t.Errorf("metaGet(key53) expected error ErrCacheMiss instead of %v", err)
	}

	//meta get with malformed key
	respMetadata, err = c.MetaGet("key val", nil)
	if err != ErrMalformedKey {
		t.Errorf("metaGet(key val) should return ErrMalformedKey instead of %v", err)
	}

	//meta get with cas response flag , ttl response flag , key response flag , Opaque token
	opaqueToken := "Opaque"
	respMetadata, err = c.MetaGet("key", &MetaGetFlags{ReturnCasTokenInResponse: true,
		ReturnTTLRemainingSecondsInResponse: true, ReturnKeyInResponse: true,
		OpaqueToken: &opaqueToken})
	checkErr(err, "cas,ttl metaGet(key): %v", err)
	if respMetadata.CasId == nil {
		t.Errorf("metaGet(key) casid should not be nil")
	}
	if respMetadata.TTLRemainingInSeconds == nil || *respMetadata.TTLRemainingInSeconds != -1 {
		t.Errorf("metaGet(key) TTLRemainingInSeconds should not be nil or should be -1 ")
	}
	if respMetadata.ItemKey == nil || *respMetadata.ItemKey != "key" {
		t.Errorf("metaGet(key) ItemKey should not be nil. Should be key")
	}
	if respMetadata.OpaqueToken == nil || *respMetadata.OpaqueToken != "Opaque" {
		t.Errorf("metaGet(key)  OpaqueToken should not be nil. Should be Opaque")
	}

	//meta get update ttl and fetch ttl
	var updateTTlToken int32 = 5000
	respMetadata, err = c.MetaGet("key", &MetaGetFlags{UpdateTTLToken: &updateTTlToken,
		ReturnTTLRemainingSecondsInResponse: true})
	checkErr(err, "ttl,update ttl metaGet(key): %v", err)
	if respMetadata.TTLRemainingInSeconds == nil || *respMetadata.TTLRemainingInSeconds != 5000 {
		t.Errorf("metaGet(key) TTLRemainingInSeconds should not be nil. should be 5000")
	}

	//test DontBumpItemInLRU flag
	key4 := &Item{Key: "key4", Value: []byte("value")}
	err = c.Set(key4)
	checkErr(err, "set(key4): %v", err)
	respMetadata, err = c.MetaGet("key4", &MetaGetFlags{DontBumpItemInLRU: true})
	checkErr(err, "metaGet(key4): %v", err)

	respMetadata, err = c.MetaGet("key4", &MetaGetFlags{ReturnLastAccessedTimeSecondsInResponse: true,
		ReturnItemHitInResponse: true})
	checkErr(err, "metaGet(key4): %v", err)
	if respMetadata.isItemHitBefore == nil || *respMetadata.isItemHitBefore == true {
		t.Errorf("metaGet(key4) isItemHitBefore should not be nil but should be false")
	}

	//testVivify TTL token
	var vivifyTTLToken int32 = 300
	respMetadata, err = c.MetaGet("key5", &MetaGetFlags{VivifyTTLToken: &vivifyTTLToken})
	checkErr(err, "metaGet(key5): %v", err)
	if respMetadata.IsReCacheWonFlagSet == nil || *respMetadata.IsReCacheWonFlagSet != true {
		t.Errorf("metaGet(key5) IsReCacheWonFlagSet should not be nil but should be true")
	}
	if string(respMetadata.ReturnItemValue) != "" {
		t.Errorf("metaGet(key5) value should be empty")
	}

	respMetadata, err = c.MetaGet("key5", &MetaGetFlags{VivifyTTLToken: &vivifyTTLToken})
	if respMetadata.IsReCacheWonFlagSet != nil ||
		respMetadata.IsReCacheWonFlagAlreadySent == nil || *respMetadata.IsReCacheWonFlagAlreadySent == false {
		t.Errorf("metaGet(key5) IsReCacheWonFlagAlreadySent should not be nil but should be true. IsReCacheWonFlagSet should be nil")
	}

	//testRecacheTTLToken
	key6 := &Item{Key: "key6", Value: []byte("value"), Expiration: 300}
	err = c.Set(key6)
	checkErr(err, "set(key6): %v", err)
	time.Sleep(2 * time.Second)

	var reCacheTTLToken int32 = 300
	respMetadata, err = c.MetaGet("key6", &MetaGetFlags{ReCacheTTLToken: &reCacheTTLToken,
		ReturnTTLRemainingSecondsInResponse: true})
	checkErr(err, "metaGet(key6): %v", err)
	if respMetadata.IsReCacheWonFlagSet == nil || *respMetadata.IsReCacheWonFlagSet != true {
		t.Errorf("metaGet(key6) IsReCacheWonFlagSet should not be nil but should be true")
	}
	respMetadata, err = c.MetaGet("key6", &MetaGetFlags{})
	checkErr(err, "metaGet(key6): %v", err)
	if respMetadata.IsReCacheWonFlagSet != nil || respMetadata.IsReCacheWonFlagAlreadySent == nil ||
		*respMetadata.IsReCacheWonFlagAlreadySent == false {
		t.Errorf("metaGet(key6) IsReCacheWonFlagSet should not be nil but should be true")
	}
}

func testTouchWithClient(t *testing.T, c *Client) {
	if testing.Short() {
		t.Log("Skipping testing memcache Touch with testing in Short mode")
		return
	}

	mustSet := mustSetF(t, c)

	const secondsToExpiry = int32(2)

	// We will set foo and bar to expire in 2 seconds, then we'll keep touching
	// foo every second
	// After 3 seconds, we expect foo to be available, and bar to be expired
	foo := &Item{Key: "foo", Value: []byte("fooval"), Expiration: secondsToExpiry}
	bar := &Item{Key: "bar", Value: []byte("barval"), Expiration: secondsToExpiry}

	setTime := time.Now()
	mustSet(foo)
	mustSet(bar)

	for s := 0; s < 3; s++ {
		time.Sleep(time.Duration(1 * time.Second))
		err := c.Touch(foo.Key, secondsToExpiry)
		if nil != err {
			t.Errorf("error touching foo: %v", err.Error())
		}
	}

	_, err := c.Get("foo")
	if err != nil {
		if err == ErrCacheMiss {
			t.Fatalf("touching failed to keep item foo alive")
		} else {
			t.Fatalf("unexpected error retrieving foo after touching: %v", err.Error())
		}
	}

	_, err = c.Get("bar")
	if nil == err {
		t.Fatalf("item bar did not expire within %v seconds", time.Now().Sub(setTime).Seconds())
	} else {
		if err != ErrCacheMiss {
			t.Fatalf("unexpected error retrieving bar: %v", err.Error())
		}
	}
}

func BenchmarkOnItem(b *testing.B) {
	fakeServer, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal("Could not open fake server: ", err)
	}
	defer fakeServer.Close()
	go func() {
		for {
			if c, err := fakeServer.Accept(); err == nil {
				go func() { io.Copy(ioutil.Discard, c) }()
			} else {
				return
			}
		}
	}()

	addr := fakeServer.Addr()
	c := New(addr.String())
	if _, err := c.getConn(addr); err != nil {
		b.Fatal("failed to initialize connection to fake server")
	}

	item := Item{Key: "foo"}
	dummyFn := func(_ *Client, _ *bufio.ReadWriter, _ *Item) error { return nil }
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.onItem(&item, dummyFn)
	}
}
