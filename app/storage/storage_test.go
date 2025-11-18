package storage

import (
	"net"
	"testing"
	"time"
)

// Helper function to create test connections
func createTestConn() net.Conn {
	c1, _ := net.Pipe()
	return c1
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

	if db.transactions == nil {
		t.Error("Expected transactions map to be initialized")
	}

	if db.subscribers == nil {
		t.Error("Expected subscribers map to be initialized")
	}

	if db.publishers == nil {
		t.Error("Expected publishers map to be initialized")
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
		stream := []*StreamEntry{NewStreamEntry("1-0")}
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
		stream := []*StreamEntry{entry}

		db.SetStream("stream1", stream)

		retrieved, ok := db.GetStream("stream1")
		if !ok {
			t.Fatal("Expected stream to exist")
		}

		if len(retrieved) != 1 {
			t.Errorf("Expected 1 entry, got %d", len(retrieved))
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

// Test Transaction operations
func TestDB_TransactionOperations(t *testing.T) {
	t.Run("CreateTransaction and GetTransaction", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.CreateTransaction(conn)

		trans, ok := db.GetTransaction(conn)
		if !ok {
			t.Fatal("Expected transaction to exist")
		}

		if len(trans) != 0 {
			t.Error("Expected empty transaction")
		}
	})

	t.Run("AddToTransaction", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.CreateTransaction(conn)
		db.AddToTransaction(conn, []byte("cmd1"))
		db.AddToTransaction(conn, []byte("cmd2"))

		trans, _ := db.GetTransaction(conn)
		if len(trans) != 2 {
			t.Errorf("Expected 2 commands, got %d", len(trans))
		}
	})

	t.Run("DeleteTransaction", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.CreateTransaction(conn)
		db.DeleteTransaction(conn)

		_, ok := db.GetTransaction(conn)
		if ok {
			t.Error("Expected transaction to be deleted")
		}
	})
}

// Test Pub/Sub operations
func TestDB_PubSubOperations(t *testing.T) {
	t.Run("AddSubscriber and GetSubscribers", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.AddSubscriber("channel1", conn)

		subs, ok := db.GetSubscribers("channel1")
		if !ok {
			t.Fatal("Expected subscribers to exist")
		}

		if !subs.Has(conn) {
			t.Error("Expected conn to be in subscribers")
		}

		if subs.Length() != 1 {
			t.Errorf("Expected 1 subscriber, got %d", subs.Length())
		}
	})

	t.Run("RemoveSubscriberFromPublisher", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.AddSubscriber("channel1", conn)
		db.RemoveSubscriberFromPublisher("channel1", conn)

		_, ok := db.GetSubscribers("channel1")
		if ok {
			t.Error("Expected subscribers to be removed")
		}
	})

	t.Run("CreateReceiver and GetReceiver", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.CreateReceiver(conn)

		ch, ok := db.GetReceiver(conn)
		if !ok {
			t.Fatal("Expected receiver to exist")
		}

		if ch == nil {
			t.Error("Expected channel to be initialized")
		}
	})

	t.Run("AddPublisher and GetPublishers", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.AddPublisher(conn, "channel1")
		db.AddPublisher(conn, "channel2")

		pubs, ok := db.GetPublishers(conn)
		if !ok {
			t.Fatal("Expected publishers to exist")
		}

		if pubs.Length() != 2 {
			t.Errorf("Expected 2 publishers, got %d", pubs.Length())
		}
	})

	t.Run("RemovePublisherFromSubscriber", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.AddPublisher(conn, "channel1")
		db.AddPublisher(conn, "channel2")
		db.RemovePublisherFromSubscriber(conn, "channel1")

		pubs, _ := db.GetPublishers(conn)
		if pubs.Length() != 1 {
			t.Errorf("Expected 1 publisher remaining, got %d", pubs.Length())
		}
	})

	t.Run("DeleteSubscriber", func(t *testing.T) {
		db := NewDB()
		conn := createTestConn()
		defer conn.Close()

		db.AddPublisher(conn, "channel1")
		db.CreateReceiver(conn)
		db.DeleteSubscriber(conn)

		_, ok1 := db.GetPublishers(conn)
		_, ok2 := db.GetReceiver(conn)

		if ok1 || ok2 {
			t.Error("Expected subscriber to be fully deleted")
		}
	})
}

// Test BLPOP waiter operations
func TestDB_BLPOPWaiters(t *testing.T) {
	t.Run("AddBLPOPWaiter and GetBLPOPWaiters", func(t *testing.T) {
		db := NewDB()

		ch := db.AddBLPOPWaiter("key1")

		if ch == nil {
			t.Fatal("Expected channel to be returned")
		}

		waiters, ok := db.GetBLPOPWaiters("key1")
		if !ok {
			t.Fatal("Expected waiters to exist")
		}

		if len(waiters) != 1 {
			t.Errorf("Expected 1 waiter, got %d", len(waiters))
		}
	})

	t.Run("PopBLPOPWaiter", func(t *testing.T) {
		db := NewDB()

		db.AddBLPOPWaiter("key1")
		db.AddBLPOPWaiter("key1")
		db.PopBLPOPWaiter("key1")

		waiters, _ := db.GetBLPOPWaiters("key1")
		if len(waiters) != 1 {
			t.Errorf("Expected 1 waiter remaining, got %d", len(waiters))
		}
	})

	t.Run("RemoveBLPOPWaiter", func(t *testing.T) {
		db := NewDB()

		db.AddBLPOPWaiter("key1")
		db.AddBLPOPWaiter("key1")
		db.RemoveBLPOPWaiter("key1", 0)

		waiters, _ := db.GetBLPOPWaiters("key1")
		if len(waiters) != 1 {
			t.Errorf("Expected 1 waiter remaining, got %d", len(waiters))
		}
	})

	t.Run("RemoveBLPOPWaiter removes key when empty", func(t *testing.T) {
		db := NewDB()

		db.AddBLPOPWaiter("key1")
		db.RemoveBLPOPWaiter("key1", 0)

		_, ok := db.GetBLPOPWaiters("key1")
		if ok {
			t.Error("Expected waiters to be removed when empty")
		}
	})
}

// Test XREAD waiter operations
func TestDB_XREADWaiters(t *testing.T) {
	t.Run("AddXREADIDWaiter and GetXREADIDWaiters", func(t *testing.T) {
		db := NewDB()
		ch := make(chan *StreamEntry)

		db.AddXREADIDWaiter("stream1", "1-0", ch)

		waiters, ok := db.GetXREADIDWaiters("stream1")
		if !ok {
			t.Fatal("Expected waiters to exist")
		}

		idWaiters := waiters["1-0"]
		if len(idWaiters) != 1 {
			t.Errorf("Expected 1 waiter for ID, got %d", len(idWaiters))
		}
	})

	t.Run("RemoveXREADIDWaiter", func(t *testing.T) {
		db := NewDB()
		ch := make(chan *StreamEntry)

		db.AddXREADIDWaiter("stream1", "1-0", ch)
		db.RemoveXREADIDWaiter("stream1", "1-0", 0)

		waiters, _ := db.GetXREADIDWaiters("stream1")
		idWaiters := waiters["1-0"]
		if len(idWaiters) != 0 {
			t.Errorf("Expected 0 waiters, got %d", len(idWaiters))
		}
	})

	t.Run("AddXREADAllWaiter and GetXREADAllWaiters", func(t *testing.T) {
		db := NewDB()
		ch := make(chan *StreamEntry)

		db.AddXREADAllWaiter("stream1", ch)

		waiters, ok := db.GetXREADAllWaiters("stream1")
		if !ok {
			t.Fatal("Expected waiters to exist")
		}

		if len(waiters) != 1 {
			t.Errorf("Expected 1 waiter, got %d", len(waiters))
		}
	})

	t.Run("RemoveXREADAllWaiter", func(t *testing.T) {
		db := NewDB()
		ch := make(chan *StreamEntry)

		db.AddXREADAllWaiter("stream1", ch)
		db.RemoveXREADAllWaiter("stream1", 0)

		waiters, _ := db.GetXREADAllWaiters("stream1")
		if len(waiters) != 0 {
			t.Errorf("Expected 0 waiters, got %d", len(waiters))
		}
	})
}

// Test Set operations
func TestSet(t *testing.T) {
	t.Run("NewSet creates empty set", func(t *testing.T) {
		s := NewSet[string]()

		if s.Length() != 0 {
			t.Error("Expected empty set")
		}
	})

	t.Run("Add and Has", func(t *testing.T) {
		s := NewSet[string]()
		s.Add("item1")

		if !s.Has("item1") {
			t.Error("Expected set to contain item1")
		}

		if s.Length() != 1 {
			t.Errorf("Expected length 1, got %d", s.Length())
		}
	})

	t.Run("Add duplicate doesn't increase length", func(t *testing.T) {
		s := NewSet[string]()
		s.Add("item1")
		s.Add("item1")

		if s.Length() != 1 {
			t.Errorf("Expected length 1, got %d", s.Length())
		}
	})

	t.Run("Remove", func(t *testing.T) {
		s := NewSet[string]()
		s.Add("item1")
		s.Remove("item1")

		if s.Has("item1") {
			t.Error("Expected item1 to be removed")
		}

		if s.Length() != 0 {
			t.Errorf("Expected length 0, got %d", s.Length())
		}
	})

	t.Run("Items returns map", func(t *testing.T) {
		s := NewSet[string]()
		s.Add("a")
		s.Add("b")

		items := s.Items()
		if len(items) != 2 {
			t.Errorf("Expected 2 items, got %d", len(items))
		}

		_, ok1 := items["a"]
		_, ok2 := items["b"]
		if !ok1 || !ok2 {
			t.Error("Expected both items in map")
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
