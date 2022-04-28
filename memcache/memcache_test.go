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
	"bytes"
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
	testMetaSetCommandsWithClient(t, c, checkErr)

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

func testMetaSetCommandsWithClient(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{})) {
	key := "bah"
	value := []byte("bahval")
	opaqueToken := "A123"
	metaFoo := &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{ReturnKeyInResponse: true, ReturnCasTokenInResponse: true, OpaqueToken: &opaqueToken}}
	response, err := c.MetaSet(metaFoo)
	if *response.ItemKey != key {
		t.Errorf("meta set(%s) Key = %q, want %s", key, *response.ItemKey, key)
	}
	if *response.OpaqueToken != opaqueToken {
		t.Errorf("meta set(%s) Opaque token = %s, want %s", key, *response.OpaqueToken, opaqueToken)
	}
	casToken := response.CasId
	if casToken == nil {
		t.Errorf("meta set(%s) error, no CAS token returned", key)
	}
	checkErr(err, "normal meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the same cas token as what was last set
	value = []byte("new_bah_val")
	var newTTL int32 = 900000
	var clientFlagToken uint32 = 90
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{CompareCasTokenToUpdateValue: casToken, ClientFlagToken: &clientFlagToken, UpdateTTLToken: &newTTL}}
	response, err = c.MetaSet(metaFoo)
	checkErr(err, "Same CAS token meta set(%s): %v", key, err)
	it, err := c.Get(key)
	if it.Flags != clientFlagToken {
		t.Errorf("Same CAS token meta set(%s) expected client flag %d but got %d", key, clientFlagToken, it.Flags)
	}
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using a different cas token
	value = []byte("byte_val_invalid")
	var newCasToken uint64 = 123456789
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{CompareCasTokenToUpdateValue: &newCasToken}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrCASConflict {
		t.Errorf("Differnet CAS token meta set(%s) expected an CAS conflict error but got %e", key, err)
	}

	// set with no reply semantics turned on
	// note that the documentation says that this flag will always return an error (even if the command runs successfully)
	value = []byte("with_base64_key")
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{UseNoReplySemanticsForResponse: true}}
	_, err = c.MetaSet(metaFoo)
	// the error raised is an internal error, so we can't change for explicit type
	if err == nil {
		t.Errorf("no reply meta set(%s) expected an error to be returned but got none", key)
	}

	// set using the append mode with existing key
	valueToAppend := []byte("append_value_to_existing")
	value = append(value, valueToAppend...)
	metaFoo = &MetaSetItem{Key: key, Value: valueToAppend, Flags: MetaSetFlags{SetModeToken: Append}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "successful append meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the prepend mode
	valueToPrepend := []byte("prepend_value_to_existing")
	value = append(valueToPrepend, value...)
	metaFoo = &MetaSetItem{Key: key, Value: valueToPrepend, Flags: MetaSetFlags{SetModeToken: Prepend}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "successful prepend meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the add mode and existing key
	// will fail to store and return ErrNotStored error because key is in use
	metaFoo = &MetaSetItem{Key: key, Value: []byte("add_value_to_existing"), Flags: MetaSetFlags{SetModeToken: Add}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("add mode with existing key meta set(%s) expected not stored error but got %e", key, err)
	}

	// set using the replace mode
	value = []byte("replacement_value")
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{SetModeToken: Replace}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "successful replace mode meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using the add mode
	// will store because key is not in use
	key = "new_key"
	value = []byte("add_value_to_new_key")
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{SetModeToken: Add}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "add mode without existing key meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, key, value)

	// set using base64 encoded string
	key = "bmV3QmFzZUtleQ=="
	decodedKey := "newBaseKey"
	value = []byte("with_base64_key")
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{IsKeyBase64: true}}
	_, err = c.MetaSet(metaFoo)
	checkErr(err, "base64 encoded key meta set(%s): %v", key, err)
	testMetaSetSavedValue(t, c, checkErr, decodedKey, value)

	// set using the append mode with non-existent key
	valueToAppend = []byte("new_append_value")
	key = "non_existing_for_append"
	metaFoo = &MetaSetItem{Key: key, Value: valueToAppend, Flags: MetaSetFlags{SetModeToken: Append}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("Append with non-existent key meta set(%s) expected a not stored error but got %e", key, err)
	}

	// set using the prepend mode with non-existent key
	valueToPrepend = []byte("new_prepend_value")
	key = "non_existing_for_prepend"
	metaFoo = &MetaSetItem{Key: key, Value: valueToPrepend, Flags: MetaSetFlags{SetModeToken: Prepend}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("Prepend with non-existent key meta set(%s) expected a not stored error but got %e", key, err)
	}

	// set using the replace mode with non-existent key
	value = []byte("new_replace_value")
	key = "non_existing_for_replace"
	metaFoo = &MetaSetItem{Key: key, Value: value, Flags: MetaSetFlags{SetModeToken: Replace}}
	_, err = c.MetaSet(metaFoo)
	if err != ErrNotStored {
		t.Errorf("Replace with non-existent key meta set(%s) expected a not stored error but got %e", key, err)
	}
}

func testMetaSetSavedValue(t *testing.T, c *Client, checkErr func(err error, format string, args ...interface{}), key string, value []byte) {
	it, err := c.Get(key)
	checkErr(err, "get(%s): %v", key, err)
	if it.Key != key {
		t.Errorf("get(%s) Key = %q, want %s", key, it.Key, key)
	}
	if bytes.Compare(it.Value, value) != 0 {
		t.Errorf("get(%s) Value = %q, want %q", key, string(it.Value), string(value))
	}
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
