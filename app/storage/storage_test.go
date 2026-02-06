package storage

import (
	"net"
	"testing"
	"time"

	"github.com/aryand15/go-redis/app/client"
)

// Helper function to create test client
func createTestClient() *client.Client {
	c1, _ := net.Pipe()
	return client.NewClient(&c1)
}

// Test NewDB
func TestNewDB(t *testing.T) {
	db := NewDB()

	if db == nil {
		t.Fatal("Expected DB to be initialized")
	}

	if db.stringData == nil {
		t.Error("Expected stringData map to be initialized")
	}

	if db.listData == nil {
		t.Error("Expected listData map to be initialized")
	}

	if db.streamData == nil {
		t.Error("Expected streamData map to be initialized")
	}

	if db.subscribers == nil {
		t.Error("Expected subscribers map to be initialized")
	}

}

// Test String operations
func TestDB_StringOperations(t *testing.T) {
	t.Run("SetString and GetString", func(t *testing.T) {
		db := NewDB()
		db.SetString("key1", "value1")

		val, ok := db.GetString("key1")
		if !ok {
			t.Fatal("Expected key to exist")
		}
		if val != "value1" {
			t.Errorf("Expected value1, got %s", val)
		}
	})

	t.Run("GetString non-existent key", func(t *testing.T) {
		db := NewDB()
		_, ok := db.GetString("missing")
		if ok {
			t.Error("Expected key not to exist")
		}
	})

	t.Run("TimedSetString expires", func(t *testing.T) {
		db := NewDB()
		db.TimedSetString("temp", "val", 100*time.Millisecond)

		// Should exist immediately
		val, ok := db.GetString("temp")
		if !ok || val != "val" {
			t.Fatal("Expected key to exist immediately")
		}

		// Wait for expiry
		time.Sleep(150 * time.Millisecond)

		_, ok = db.GetString("temp")
		if ok {
			t.Error("Expected key to be expired")
		}
	})

	t.Run("SetString replaces timer", func(t *testing.T) {
		db := NewDB()
		db.TimedSetString("key", "old", 1*time.Second)
		db.SetString("key", "new")

		time.Sleep(1100 * time.Millisecond)

		// Key should still exist since timer was replaced
		val, ok := db.GetString("key")
		if !ok {
			t.Error("Expected key to still exist")
		}
		if val != "new" {
			t.Errorf("Expected 'new', got %s", val)
		}
	})

	t.Run("CanSetString checks conflicts", func(t *testing.T) {
		db := NewDB()

		// Can set on empty key
		if !db.CanSetString("newkey") {
			t.Error("Expected to be able to set string on empty key")
		}

		// Cannot set when list exists
		db.SetList("listkey", []string{"a"})
		if db.CanSetString("listkey") {
			t.Error("Expected not to be able to set string when list exists")
		}

		// Cannot set when stream exists
		stream := NewStream()
		stream.AddEntry(NewStreamEntry("1-0"))
		db.SetStream("streamkey", stream)
		if db.CanSetString("streamkey") {
			t.Error("Expected not to be able to set string when stream exists")
		}
	})
}

// Test List operations
func TestDB_ListOperations(t *testing.T) {
	t.Run("SetList and GetList", func(t *testing.T) {
		db := NewDB()
		list := []string{"a", "b", "c"}
		db.SetList("mylist", list)

		retrieved, ok := db.GetList("mylist")
		if !ok {
			t.Fatal("Expected list to exist")
		}

		if len(retrieved) != 3 {
			t.Errorf("Expected length 3, got %d", len(retrieved))
		}

		if retrieved[0] != "a" || retrieved[1] != "b" || retrieved[2] != "c" {
			t.Error("List contents don't match")
		}
	})

	t.Run("AppendToList", func(t *testing.T) {
		db := NewDB()
		db.SetList("list", []string{"a"})
		db.AppendToList("list", "b")

		list, _ := db.GetList("list")
		if len(list) != 2 || list[1] != "b" {
			t.Error("Append failed")
		}
	})

	t.Run("PrependToList", func(t *testing.T) {
		db := NewDB()
		db.SetList("list", []string{"b"})
		db.PrependToList("list", "a")
		list, _ := db.GetList("list")
		if len(list) != 2 || list[0] != "a" {
			t.Error("Prepend failed")
		}
	})

	t.Run("PopLeftList", func(t *testing.T) {
		db := NewDB()
		db.SetList("list", []string{"a", "b", "c"})
		db.PopLeftList("list")

		list, _ := db.GetList("list")
		if len(list) != 2 || list[0] != "b" {
			t.Error("PopLeft failed")
		}
	})

	t.Run("CanSetList checks conflicts", func(t *testing.T) {
		db := NewDB()

		// Can set on empty key
		if !db.CanSetList("newkey") {
			t.Error("Expected to be able to set list on empty key")
		}

		// Cannot set when string exists
		db.SetString("stringkey", "val")
		if db.CanSetList("stringkey") {
			t.Error("Expected not to be able to set list when string exists")
		}
	})
}

// Test Stream operations
func TestDB_StreamOperations(t *testing.T) {
	t.Run("SetStream and GetStream", func(t *testing.T) {
		db := NewDB()
		entry := NewStreamEntry("1-0")
		entry.Set("field", "value")
		stream := NewStream()
		stream.AddEntry(entry)

		db.SetStream("stream1", stream)

		retrieved, ok := db.GetStream("stream1")
		if !ok {
			t.Fatal("Expected stream to exist")
		}

		if retrieved.Length() != 1 {
			t.Errorf("Expected 1 entry, got %d", retrieved.Length())
		}
	})

	t.Run("CanSetStream checks conflicts", func(t *testing.T) {
		db := NewDB()

		// Can set on empty key
		if !db.CanSetStream("newkey") {
			t.Error("Expected to be able to set stream on empty key")
		}

		// Cannot set when string exists
		db.SetString("stringkey", "val")
		if db.CanSetStream("stringkey") {
			t.Error("Expected not to be able to set stream when string exists")
		}
	})
}

// Test Pub/Sub operations
func TestDB_PubSubOperations(t *testing.T) {
	t.Run("AddSubscriber and GetSubscribers", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		c.SetSubscribeMode(true)
		db.AddSubscriber(c, "channel1")

		subs, ok := db.GetSubscribers("channel1")
		if !ok {
			t.Fatal("Expected subscribers to exist")
		}

		if !subs.Has(c) {
			t.Error("Expected client to be in subscribers")
		}

		if subs.Length() != 1 {
			t.Errorf("Expected 1 subscriber, got %d", subs.Length())
		}
	})

	t.Run("RemoveSubscriber", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		c.SetSubscribeMode(true)
		db.AddSubscriber(c, "channel1")
		db.RemoveSubscriber(c, "channel1")

		_, ok := db.GetSubscribers("channel1")
		if ok {
			t.Error("Expected subscribers to be removed")
		}
	})
}

// Test BLPOP waiter operations
func TestDB_BLPOPWaiters(t *testing.T) {
	t.Run("AddBLPOPWaiter and GetNextBLPOPWaiter", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		db.AddBLPOPWaiter("key1", c)

		waiter, ok := db.GetNextBLPOPWaiter("key1")
		if !ok {
			t.Fatal("Expected waiters to exist")
		}
		if waiter != c {
			t.Error("Expected to get the same client back")
		}
	})

	t.Run("PopBLPOPWaiter", func(t *testing.T) {
		db := NewDB()
		c1 := createTestClient()
		c2 := createTestClient()
		defer c1.CloseConn()
		defer c2.CloseConn()

		db.AddBLPOPWaiter("key1", c1)
		db.AddBLPOPWaiter("key1", c2)
		db.PopBLPOPWaiter("key1", c1)

		waiter, ok := db.GetNextBLPOPWaiter("key1")
		if !ok {
			t.Fatal("Expected waiter to exist")
		}
		if waiter != c2 {
			t.Error("Expected c2 to be next waiter after popping c1")
		}
	})

	t.Run("RemoveBLPOPWaiter", func(t *testing.T) {
		db := NewDB()
		c1 := createTestClient()
		c2 := createTestClient()
		defer c1.CloseConn()
		defer c2.CloseConn()

		db.AddBLPOPWaiter("key1", c1)
		db.AddBLPOPWaiter("key1", c2)
		db.RemoveBLPOPWaiter("key1", c1)

		waiter, ok := db.GetNextBLPOPWaiter("key1")
		if !ok {
			t.Fatal("Expected waiter to exist")
		}
		if waiter != c2 {
			t.Error("Expected c2 to be next waiter after removing c1")
		}
	})
}

// Test XREAD waiter operations
func TestDB_XREADWaiters(t *testing.T) {
	t.Run("AddXREADIDWaiter and GetXREADIDWaiters", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		db.AddXREADIDWaiter("stream1", "1-0", c)

		waiters, ok := db.GetXREADIDWaiters("stream1")
		if !ok {
			t.Fatal("Expected waiters to exist")
		}

		idWaiters := waiters["1-0"]
		if idWaiters.Length() != 1 {
			t.Errorf("Expected 1 waiter for ID, got %d", idWaiters.Length())
		}
	})

	t.Run("RemoveXREADIDWaiter", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		db.AddXREADIDWaiter("stream1", "1-0", c)
		db.RemoveXREADIDWaiter("stream1", "1-0", c)

		waiters, ok := db.GetXREADIDWaiters("stream1")
		if !ok {
			// Stream info may still exist but with no waiters for this ID
			return
		}
		idWaiters, exists := waiters["1-0"]
		if exists && idWaiters.Length() != 0 {
			t.Errorf("Expected 0 waiters, got %d", idWaiters.Length())
		}
	})

	t.Run("AddXREADAllWaiter and GetXREADAllWaiters", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		// First create a stream so GetXREADAllWaiters doesn't error
		stream := NewStream()
		stream.AddEntry(NewStreamEntry("0-1"))
		db.SetStream("stream1", stream)

		db.AddXREADAllWaiter("stream1", c)

		waiters, err := db.GetXREADAllWaiters("stream1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if waiters.Length() != 1 {
			t.Errorf("Expected 1 waiter, got %d", waiters.Length())
		}
	})

	t.Run("RemoveXREADAllWaiter", func(t *testing.T) {
		db := NewDB()
		c := createTestClient()
		defer c.CloseConn()

		// First create a stream
		stream := NewStream()
		stream.AddEntry(NewStreamEntry("0-1"))
		db.SetStream("stream1", stream)

		db.AddXREADAllWaiter("stream1", c)
		db.RemoveXREADAllWaiter("stream1", c)

		waiters, err := db.GetXREADAllWaiters("stream1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if waiters.Length() != 0 {
			t.Errorf("Expected 0 waiters, got %d", waiters.Length())
		}
	})
}

// Test StreamEntry
func TestStreamEntry(t *testing.T) {
	t.Run("NewStreamEntry", func(t *testing.T) {
		entry := NewStreamEntry("123-456")

		if entry.GetID() != "123-456" {
			t.Errorf("Expected ID '123-456', got '%s'", entry.GetID())
		}
	})

	t.Run("Set and values", func(t *testing.T) {
		entry := NewStreamEntry("1-0")
		entry.Set("field1", "value1")
		entry.Set("field2", "value2")

		if len(entry.values) != 2 {
			t.Errorf("Expected 2 values, got %d", len(entry.values))
		}

		if entry.values["field1"] != "value1" {
			t.Error("field1 value mismatch")
		}
	})

	t.Run("GetMillis and GetSeqNum", func(t *testing.T) {
		entry := NewStreamEntry("123-456")

		if entry.GetMillis() != 123 {
			t.Errorf("Expected millis 123, got %d", entry.GetMillis())
		}

		if entry.GetSeqNum() != 456 {
			t.Errorf("Expected seqNum 456, got %d", entry.GetSeqNum())
		}
	})

	t.Run("RESPData conversion", func(t *testing.T) {
		entry := NewStreamEntry("1-0")
		entry.Set("field", "value")

		resp := entry.RESPData()

		if len(resp.ListRESPData) != 2 {
			t.Errorf("Expected 2 elements in RESP, got %d", len(resp.ListRESPData))
		}

		// First element should be ID
		if string(resp.ListRESPData[0].Data) != "1-0" {
			t.Error("ID mismatch in RESP")
		}

		// Second element should be key-value pairs
		kvList := resp.ListRESPData[1]
		if len(kvList.ListRESPData) != 2 {
			t.Errorf("Expected 2 k-v elements, got %d", len(kvList.ListRESPData))
		}
	})
}

// Test concurrency
func TestDB_Concurrency(t *testing.T) {
	t.Run("concurrent string operations", func(t *testing.T) {
		db := NewDB()
		done := make(chan bool)

		for i := 0; i < 10; i++ {
			go func(idx int) {
				key := "key"
				db.Lock()
				db.SetString(key, "value")
				_, _ = db.GetString(key)
				db.Unlock()
				done <- true
			}(i)
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}
