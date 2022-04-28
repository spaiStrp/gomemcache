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

	// test meta commands
	// testMetaCommandsWithClient(t, c, checkErr)
	testMetaDeleteCommandsWithClient(t, c, checkErr)

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

func testMetaCommandsWithClient(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{})) {
	// Meta Set
	key := "bah"
	opaqueToken := "A123"
	metaFoo := &MetaSetItem{Key: key, Value: []byte("bahval"), Flags: MetaSetFlags{ReturnKeyInResponse: true, ReturnCasTokenInResponse: true, OpaqueToken: &opaqueToken}}
	response, err := c.MetaSet(metaFoo)
	if *response.Key != key {
		t.Errorf("meta set(%s) Key = %q, want %s", key, *response.Key, key)
	}
	if *response.OpaqueValue != opaqueToken {
		t.Errorf("meta set(%s) Opaque token = %s, want %s", key, *response.Key, opaqueToken)
	}
	casToken := response.CasId
	if casToken == nil {
		t.Errorf("meta set(%s) error, no CAS token returned", key)
	}
	checkErr(err, "first meta set(%s): %v", key, err)

	// set using the same cas token as what was last set
	var newTTL int32 = 900000
	var clientFlagToken uint32 = 90
	metaFoo = &MetaSetItem{Key: key, Value: []byte("new_bah_val"), Flags: MetaSetFlags{CompareCasTokenToUpdateValue: casToken, ClientFlagToken: &clientFlagToken, UpdateTTLToken: &newTTL}}
	response, err = c.MetaSet(metaFoo)
	checkErr(err, "second meta set(%s): %v", key, err)

	// set with no reply semantics turned on
	// note that the documentation says that this flag will always return an error (even if the command runs successfully)
	metaFoo = &MetaSetItem{Key: key, Value: []byte("with_base64_key"), Flags: MetaSetFlags{UseNoReplySemanticsForResponse: true}}
	_, err = c.MetaSet(metaFoo)
	if err == nil {
		t.Errorf("third meta set(%s) expected an error to be returned but got none", key)
	}

	// set using the append mode
	metaFoo = &MetaSetItem{Key: key, Value: []byte("append_value_to_existing"), Flags: MetaSetFlags{SetModeToken: Append}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "fourth meta set(%s): %v", key, err)

	// set using the prepend mode
	metaFoo = &MetaSetItem{Key: key, Value: []byte("prepend_value_to_existing"), Flags: MetaSetFlags{SetModeToken: Prepend}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "fifth meta set(%s): %v", key, err)

	// set using the add mode and existing key
	// will fail to store and return ErrNotStored error because key is in use
	metaFoo = &MetaSetItem{Key: key, Value: []byte("add_value_to_existing"), Flags: MetaSetFlags{SetModeToken: Add}}
	_, err = c.MetaSet(metaFoo)
	if err == nil {
		t.Errorf("sixth meta set(%s) expected an error to be returned but got none", key)
	}

	// set using the replace mode
	metaFoo = &MetaSetItem{Key: key, Value: []byte("add_value_to_existing"), Flags: MetaSetFlags{SetModeToken: Replace}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "seventh meta set(%s): %v", key, err)

	// set using the add mode
	// will fail to store and return ErrNotStored error because key is in use
	metaFoo = &MetaSetItem{Key: "new_key", Value: []byte("add_value_to_existing"), Flags: MetaSetFlags{SetModeToken: Add}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "eighth meta set(%s): %v", key, err)

	// set using base64 encoded string (non-encoded is newBaseKey)
	key = "bmV3QmFzZUtleQ=="
	metaFoo = &MetaSetItem{Key: key, Value: []byte("with_base64_key"), Flags: MetaSetFlags{IsKeyBase64: true}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "ninth meta set(%s): %v", key, err)
}

func testMetaDeleteCommandsWithClient(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{})) {
	setForDelete := func(key string, value []byte, flags MetaSetFlags) (metaDataResponse *MetaResponseMetadata, err error) {
		metaSetItem := &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{ReturnCasTokenInResponse: true}}
		return c.MetaSet(metaSetItem)
	}

	key := "foo_key"
	value := []byte("foo_val")
	set_response, err := setForDelete(key, value, MetaSetFlags{ReturnCasTokenInResponse: true})
	casValue := set_response.CasId

	// normal delete with key return and opaque token provided on matching CAS token
	opaqueToken := "opaque_token"
	metaDeleteItem := &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{ReturnKeyInResponse: true, OpaqueToken: &opaqueToken, CompareCasTokenToUpdateValue: casValue}}
	response, err := c.MetaDelete(metaDeleteItem)
	if *response.Key != key {
		t.Errorf("meta delete(%s) Key = %q, want %s", key, *response.Key, key)
	}
	if *response.OpaqueValue != opaqueToken {
		t.Errorf("meta delete(%s) Opaque token = %s, want %s", key, *response.OpaqueValue, opaqueToken)
	}
	checkErr(err, "normal meta delete(%s): %v", key, err)

	// failed delete due to mismatched CAS token
	var mismatchCasToken uint64 = 123456
	set_response, err = setForDelete(key, value, MetaSetFlags{})
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{ReturnKeyInResponse: true, OpaqueToken: &opaqueToken, CompareCasTokenToUpdateValue: &mismatchCasToken}}
	_, err = c.MetaDelete(metaDeleteItem)
	if err != ErrCASConflict {
		t.Errorf("Different CAS token meta delete(%s) expected an CAS conflict error but got %e", key, err)
	}

	// TODO: requires meta get to retrieve the cas token of the set value and TTL after doing an invalidation delete
	// delete with invalidation and TTL update- expect to see a new CAS token and TTL set
	// set_response, err = setForDelete(key, value, MetaSetFlags{})
	// casValue = set_response.CasId
	// var newTTL int32 = 5000
	// metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{Invalidate: true, UpdateTTLToken: &newTTL}}
	// response, err = c.MetaDelete(metaDeleteItem)
	// if response.CasId == nil || casValue == response.CasId {
	// 	t.Errorf("Invalidate meta delete(%s) expected CAS tokens to differ but were the same", key)
	// }
	// if response.TTLRemainingInSeconds == nil || response.TTLRemainingInSeconds != &newTTL {
	// 	t.Errorf("meta delete(%s) TTLRemainingInSeconds should be non-nil and equal to %d", key, newTTL)
	// }

	// delete with no-reply semantics
	// note that the documentation says that this flag will always return an error (even if the command runs successfully)
	set_response, err = setForDelete(key, value, MetaSetFlags{})
	metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{ReturnKeyInResponse: true, UseNoReplySemanticsForResponse: true}}
	_, err = c.MetaDelete(metaDeleteItem)
	// the error raised is an internal error, so we can't check for explicit type
	if err == nil {
		t.Errorf("no reply meta delete(%s) expected an error to be returned but got none", key)
	}

	// delete with base-64 encoded key
	// key = "bmV3QmFzZUtleQ=="
	// decodedKey := "newBaseKey"
	// set_response, err = setForDelete(key, value, MetaSetFlags{IsKeyBase64: true, ReturnKeyInResponse: true})
	// metaDeleteItem = &MetaDeleteItem{Key: key, Flags: MetaDeleteFlags{IsKeyBase64: true, ReturnKeyInResponse: true}}
	// response, err = c.MetaDelete(metaDeleteItem)
	// if response.Key == nil {
	// 	t.Errorf("base-64 encoded key meta delete(%s) expected decoded key to be %s but was nil", key, decodedKey)
	// }
	// if *response.Key != decodedKey {
	// 	t.Errorf("base-64 encoded key meta delete(%s) expected decoded key to be %s but was %s", key, decodedKey, *response.Key)
	// }
	// checkErr(err, "base-64 encoded key meta delete(%s): %v", key, err)
}

func stringSlicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
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
