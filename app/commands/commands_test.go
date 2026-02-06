package commands

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aryand15/go-redis/app/client"
	"github.com/aryand15/go-redis/app/storage"
	"github.com/aryand15/go-redis/resp"
)

// helpers
func setupTestHandler() *CommandHandler {
	return NewCommandHandler(storage.NewDB())
}

// createTestClient creates a test client using net.Pipe
func createTestClient() *client.Client {
	c1, _ := net.Pipe()
	return client.NewClient(&c1)
}

func cmd(args ...string) *resp.RESPData {
	list := make([]*resp.RESPData, len(args))
	for i, a := range args {
		list[i] = resp.ConvertBulkStringToRESP(a)
	}
	return &resp.RESPData{Type: resp.Array, ListRESPData: list}
}

// Test PING and ECHO
func TestHandlePING(t *testing.T) {
	t.Run("PING returns PONG", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		r, err := h.Handle(cmd("PING"), c)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.Type != resp.SimpleString || string(r.Data) != "PONG" {
			t.Fatalf("expected simple string PONG, got: %#v", r)
		}
	})
}

func TestHandleECHO(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    string
		wantErr bool
	}{
		{"echo hello", []string{"ECHO", "hello"}, "hello", false},
		{"missing arg", []string{"ECHO"}, "", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := setupTestHandler()
			client := createTestClient()
			defer client.CloseConn()

			r, err := h.Handle(cmd(c.args...), client)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Type != resp.BulkString || string(r.Data) != c.want {
				t.Fatalf("expected bulk %q, got %#v", c.want, r)
			}
		})
	}
}

// SET/GET and EX/PX behavior plus type errors
func TestHandleSET_GET(t *testing.T) {
	t.Run("basic set-get", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// SET k v
		if _, err := h.Handle(cmd("SET", "k", "v"), c); err != nil {
			t.Fatalf("SET error: %v", err)
		}
		// GET k
		r, err := h.Handle(cmd("GET", "k"), c)
		if err != nil {
			t.Fatalf("GET error: %v", err)
		}
		if r.Type != resp.BulkString || string(r.Data) != "v" {
			t.Fatalf("unexpected GET response: %#v", r)
		}
	})

	t.Run("EX expiry works", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("SET", "k2", "v2", "EX", "1"), c); err != nil {
			t.Fatalf("SET EX error: %v", err)
		}
		// Immediately present
		r, _ := h.Handle(cmd("GET", "k2"), c)
		if r.Type != resp.BulkString || string(r.Data) != "v2" {
			t.Fatalf("expected v2 present, got %#v", r)
		}
		// wait >1s and ensure gone
		time.Sleep(1200 * time.Millisecond)
		r2, _ := h.Handle(cmd("GET", "k2"), c)
		if r2.Type != resp.BulkString || len(r2.Data) != 0 {
			t.Fatalf("expected nil bulk after expiry, got %#v", r2)
		}
	})

	t.Run("wrong type error", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// make a list at key
		if _, err := h.Handle(cmd("RPUSH", "L", "a"), c); err != nil {
			t.Fatalf("RPUSH error: %v", err)
		}
		if _, err := h.Handle(cmd("SET", "L", "x"), c); err == nil {
			t.Fatalf("expected error when setting key with list type")
		}
	})
}

// INCR concurrency and error
func TestHandleINCR(t *testing.T) {
	t.Run("concurrent incr", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// ensure starts at 0
		if _, err := h.Handle(cmd("SET", "num", "0"), c); err != nil {
			t.Fatalf("SET error: %v", err)
		}
		var wg sync.WaitGroup
		goroutines := 50
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				testClient := createTestClient()
				defer testClient.CloseConn()
				if _, err := h.Handle(cmd("INCR", "num"), testClient); err != nil {
					t.Errorf("INCR error: %v", err)
				}
			}()
		}
		wg.Wait()
		r, err := h.Handle(cmd("GET", "num"), c)
		if err != nil {
			t.Fatalf("GET error: %v", err)
		}
		if string(r.Data) != "50" {
			t.Fatalf("expected 50, got %q", string(r.Data))
		}
	})

	t.Run("incr non-int error", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("SET", "bad", "notint"), c); err != nil {
			t.Fatalf("SET error: %v", err)
		}
		if _, err := h.Handle(cmd("INCR", "bad"), c); err == nil {
			t.Fatalf("expected error when incr non-int")
		}
	})
}

// List commands and BLPOP blocking behavior
func TestListCommandsAndBLPOP(t *testing.T) {
	t.Run("rpush lpop llen lrange", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("RPUSH", "arr", "a", "b", "c"), c); err != nil {
			t.Fatalf("RPUSH error: %v", err)
		}
		rlen, _ := h.Handle(cmd("LLEN", "arr"), c)
		if rlen.Type != resp.Integer {
			t.Fatalf("expected integer reply for LLEN, got %#v", rlen)
		}
		ival, err := rlen.Int()
		if err != nil || ival != 3 {
			t.Fatalf("expected length 3, got %#v (err=%v)", rlen, err)
		}
		lr, _ := h.Handle(cmd("LRANGE", "arr", "0", "-1"), c)
		if lr.Type != resp.Array || len(lr.ListRESPData) != 3 {
			t.Fatalf("expected 3 elements, got %#v", lr)
		}
		// LPOP single
		lp, _ := h.Handle(cmd("LPOP", "arr"), c)
		if lp.Type != resp.BulkString || string(lp.Data) != "a" {
			t.Fatalf("expected 'a', got %#v", lp)
		}
	})

	t.Run("blpop blocks then returns", func(t *testing.T) {
		h := setupTestHandler()
		blpopClient := createTestClient()
		defer blpopClient.CloseConn()

		// Start BLPOP with blocking timeout >0 in goroutine
		resCh := make(chan *resp.RESPData, 1)
		go func() {
			r, _ := h.Handle(cmd("BLPOP", "bq", "5"), blpopClient)
			resCh <- r
		}()
		// Give the BLPOP a moment to register
		time.Sleep(50 * time.Millisecond)

		// Push an item with a different client
		pushClient := createTestClient()
		defer pushClient.CloseConn()
		if _, err := h.Handle(cmd("RPUSH", "bq", "X"), pushClient); err != nil {
			t.Fatalf("RPUSH error: %v", err)
		}
		select {
		case r := <-resCh:
			if r == nil || r.Type != resp.Array || len(r.ListRESPData) != 2 {
				t.Fatalf("unexpected BLPOP result: %#v", r)
			}
			if string(r.ListRESPData[1].Data) != "X" {
				t.Fatalf("expected X, got %#v", r)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for BLPOP result")
		}
	})
}

// Pub/Sub subscribe and publish
func TestPubSubSubscribePublish(t *testing.T) {
	h := setupTestHandler()
	c := createTestClient()
	defer c.CloseConn()

	// Subscribe using client
	subResp, err := h.HandleSUBSCRIBE([]*resp.RESPData{resp.ConvertBulkStringToRESP("SUBSCRIBE"), resp.ConvertBulkStringToRESP("chan")}, c)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	if subResp == nil {
		t.Fatalf("expected subscribe response")
	}

	// After subscribe, publish should return 1 subscriber
	pubResp, err := h.HandlePUBLISH([]*resp.RESPData{resp.ConvertBulkStringToRESP("PUBLISH"), resp.ConvertBulkStringToRESP("chan"), resp.ConvertBulkStringToRESP("hello")})
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}
	if pubResp.Type != resp.Integer {
		t.Fatalf("expected integer publish count, got %#v", pubResp)
	}
	pval, perr := pubResp.Int()
	if perr != nil || pval != 1 {
		t.Fatalf("expected publish count 1, got %#v (err=%v)", pubResp, perr)
	}

	// Verify message arrives on client's PubSubChan
	select {
	case msg := <-c.PubSubChan:
		if msg != "hello" {
			t.Fatalf("expected message 'hello', got %q", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for published message")
	}
}

// Transactions: MULTI, QUEUED, EXEC and DISCARD
func TestTransactions(t *testing.T) {
	t.Run("multi exec executes queued commands", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// MULTI
		if _, err := h.Handle(cmd("MULTI"), c); err != nil {
			t.Fatalf("MULTI error: %v", err)
		}
		// QUEUE a set - Handle will queue since client is in transaction mode
		queueResp, err := h.Handle(cmd("SET", "txk", "1"), c)
		if err != nil {
			t.Fatalf("queue SET error: %v", err)
		}
		if queueResp.Type != resp.SimpleString || string(queueResp.Data) != "QUEUED" {
			t.Fatalf("expected QUEUED response, got %#v", queueResp)
		}
		// EXEC
		res, err := h.Handle(cmd("EXEC"), c)
		if err != nil {
			t.Fatalf("EXEC error: %v", err)
		}
		if res.Type != resp.Array {
			t.Fatalf("expected array of results, got %#v", res)
		}
		// Ensure key set
		r, _ := h.Handle(cmd("GET", "txk"), c)
		if string(r.Data) != "1" {
			t.Fatalf("expected txk=1, got %q", string(r.Data))
		}
	})

	t.Run("multi discard does not execute", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("MULTI"), c); err != nil {
			t.Fatalf("MULTI error: %v", err)
		}
		// Queue a command
		if _, err := h.Handle(cmd("SET", "txk2", "1"), c); err != nil {
			t.Fatalf("queue SET error: %v", err)
		}
		if _, err := h.Handle(cmd("DISCARD"), c); err != nil {
			t.Fatalf("DISCARD error: %v", err)
		}
		r, _ := h.Handle(cmd("GET", "txk2"), c)
		// Expect nil bulk
		if r.Type != resp.BulkString || len(r.Data) != 0 {
			t.Fatalf("expected no key after DISCARD, got %#v", r)
		}
	})
}

// Streams: XADD + XREAD blocking
func TestStreamsXADD_XREAD(t *testing.T) {
	t.Run("xadd and xrange basic", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// XADD s1 * f v
		res, err := h.Handle(cmd("XADD", "s1", "*", "f", "v"), c)
		if err != nil {
			t.Fatalf("XADD error: %v", err)
		}
		if res.Type != resp.BulkString {
			t.Fatalf("expected id bulk, got %#v", res)
		}
		id := string(res.Data)
		// Use explicit id bounds to avoid edge-case handling paths.
		xr, err := h.Handle(cmd("XRANGE", "s1", id, id), c)
		if err != nil {
			t.Fatalf("XRANGE error: %v", err)
		}
		if xr.Type != resp.Array || len(xr.ListRESPData) != 1 {
			t.Fatalf("expected one entry from XRANGE, got %#v", xr)
		}
	})

	t.Run("xread blocking wakes on xadd", func(t *testing.T) {
		h := setupTestHandler()
		xreadClient := createTestClient()
		defer xreadClient.CloseConn()

		// Start XREAD BLOCK 500 STREAMS s2 0-0 in goroutine
		outCh := make(chan *resp.RESPData, 1)
		go func() {
			r, _ := h.Handle(cmd("XREAD", "BLOCK", "500", "STREAMS", "s2", "0-0"), xreadClient)
			outCh <- r
		}()
		// Give the XREAD a moment to register
		time.Sleep(50 * time.Millisecond)

		// Now XADD should wake it
		addClient := createTestClient()
		defer addClient.CloseConn()
		if _, err := h.Handle(cmd("XADD", "s2", "*", "k", "v"), addClient); err != nil {
			t.Fatalf("XADD error: %v", err)
		}
		select {
		case r := <-outCh:
			if r == nil || r.Type != resp.Array {
				t.Fatalf("unexpected XREAD result: %#v", r)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for XREAD result")
		}
	})
}

// Additional TYPE command tests
func TestHandleTYPE(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(*CommandHandler, *client.Client)
		key      string
		wantType string
	}{
		{
			name:     "string type",
			setup:    func(h *CommandHandler, c *client.Client) { h.Handle(cmd("SET", "k", "v"), c) },
			key:      "k",
			wantType: "string",
		},
		{
			name:     "list type",
			setup:    func(h *CommandHandler, c *client.Client) { h.Handle(cmd("RPUSH", "L", "a"), c) },
			key:      "L",
			wantType: "list",
		},
		{
			name:     "stream type",
			setup:    func(h *CommandHandler, c *client.Client) { h.Handle(cmd("XADD", "s", "*", "f", "v"), c) },
			key:      "s",
			wantType: "stream",
		},
		{
			name:     "none for missing key",
			setup:    func(h *CommandHandler, c *client.Client) {},
			key:      "missing",
			wantType: "none",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := setupTestHandler()
			c := createTestClient()
			defer c.CloseConn()

			tc.setup(h, c)
			r, err := h.Handle(cmd("TYPE", tc.key), c)
			if err != nil {
				t.Fatalf("TYPE error: %v", err)
			}
			if r.Type != resp.SimpleString || string(r.Data) != tc.wantType {
				t.Fatalf("expected type %q, got %#v", tc.wantType, r)
			}
		})
	}
}

// Additional SET/GET tests for PX option
func TestHandleSET_PX(t *testing.T) {
	t.Run("PX expiry works", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("SET", "k3", "v3", "PX", "500"), c); err != nil {
			t.Fatalf("SET PX error: %v", err)
		}
		// Immediately present
		r, _ := h.Handle(cmd("GET", "k3"), c)
		if r.Type != resp.BulkString || string(r.Data) != "v3" {
			t.Fatalf("expected v3 present, got %#v", r)
		}
		// wait >500ms and ensure gone
		time.Sleep(600 * time.Millisecond)
		r2, _ := h.Handle(cmd("GET", "k3"), c)
		if r2.Type != resp.BulkString || len(r2.Data) != 0 {
			t.Fatalf("expected nil bulk after expiry, got %#v", r2)
		}
	})

	t.Run("SET with invalid expiry option", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("SET", "k", "v", "INVALID", "100"), c); err == nil {
			t.Fatalf("expected error for invalid expiry option")
		}
	})

	t.Run("SET with negative duration", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("SET", "k", "v", "EX", "-1"), c); err == nil {
			t.Fatalf("expected error for negative duration")
		}
	})
}

// Additional LPUSH tests
func TestHandleLPUSH(t *testing.T) {
	t.Run("lpush creates list and prepends", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		r, err := h.Handle(cmd("LPUSH", "L", "a", "b", "c"), c)
		if err != nil {
			t.Fatalf("LPUSH error: %v", err)
		}
		if r.Type != resp.Integer {
			t.Fatalf("expected integer, got %#v", r)
		}
		// Should have length 3
		ival, _ := r.Int()
		if ival != 3 {
			t.Fatalf("expected length 3, got %d", ival)
		}
		// Order should be c, b, a (prepended in order)
		lr, _ := h.Handle(cmd("LRANGE", "L", "0", "-1"), c)
		if len(lr.ListRESPData) != 3 {
			t.Fatalf("expected 3 elements")
		}
		if string(lr.ListRESPData[0].Data) != "c" || string(lr.ListRESPData[1].Data) != "b" || string(lr.ListRESPData[2].Data) != "a" {
			t.Fatalf("unexpected order: %#v", lr)
		}
	})

	t.Run("lpush wrong type error", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("SET", "str", "val"), c)
		if _, err := h.Handle(cmd("LPUSH", "str", "x"), c); err == nil {
			t.Fatalf("expected error when lpush on string key")
		}
	})
}

// Additional LPOP tests with count parameter
func TestHandleLPOP_WithCount(t *testing.T) {
	t.Run("lpop with count", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("RPUSH", "L", "a", "b", "c", "d"), c)
		r, err := h.Handle(cmd("LPOP", "L", "2"), c)
		if err != nil {
			t.Fatalf("LPOP error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 2 {
			t.Fatalf("expected array of 2 elements, got %#v", r)
		}
		if string(r.ListRESPData[0].Data) != "a" || string(r.ListRESPData[1].Data) != "b" {
			t.Fatalf("unexpected popped values: %#v", r)
		}
		// Should have 2 remaining
		lr, _ := h.Handle(cmd("LRANGE", "L", "0", "-1"), c)
		if len(lr.ListRESPData) != 2 {
			t.Fatalf("expected 2 remaining elements")
		}
	})

	t.Run("lpop count exceeds length", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("RPUSH", "L", "a", "b"), c)
		r, err := h.Handle(cmd("LPOP", "L", "5"), c)
		if err != nil {
			t.Fatalf("LPOP error: %v", err)
		}
		// Should return only 2 elements
		if r.Type != resp.Array || len(r.ListRESPData) != 2 {
			t.Fatalf("expected array of 2 elements, got %#v", r)
		}
	})

	t.Run("lpop on missing key", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		r, err := h.Handle(cmd("LPOP", "missing"), c)
		if err != nil {
			t.Fatalf("LPOP error: %v", err)
		}
		if r.Type != resp.BulkString || len(r.Data) != 0 {
			t.Fatalf("expected nil bulk string, got %#v", r)
		}
	})
}

// Additional LRANGE tests for edge cases
func TestHandleLRANGE_EdgeCases(t *testing.T) {
	t.Run("lrange negative indices", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("RPUSH", "L", "a", "b", "c", "d"), c)
		r, err := h.Handle(cmd("LRANGE", "L", "-2", "-1"), c)
		if err != nil {
			t.Fatalf("LRANGE error: %v", err)
		}
		if len(r.ListRESPData) != 2 {
			t.Fatalf("expected 2 elements, got %#v", r)
		}
		if string(r.ListRESPData[0].Data) != "c" || string(r.ListRESPData[1].Data) != "d" {
			t.Fatalf("unexpected values: %#v", r)
		}
	})

	t.Run("lrange start > stop", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("RPUSH", "L", "a", "b", "c"), c)
		r, err := h.Handle(cmd("LRANGE", "L", "2", "1"), c)
		if err != nil {
			t.Fatalf("LRANGE error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 0 {
			t.Fatalf("expected empty array, got %#v", r)
		}
	})

	t.Run("lrange on missing key", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		r, err := h.Handle(cmd("LRANGE", "missing", "0", "-1"), c)
		if err != nil {
			t.Fatalf("LRANGE error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 0 {
			t.Fatalf("expected empty array, got %#v", r)
		}
	})
}

// Additional BLPOP tests
func TestHandleBLPOP_Timeout(t *testing.T) {
	t.Run("blpop timeout returns nil", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		start := time.Now()
		r, err := h.Handle(cmd("BLPOP", "empty", "0.2"), c)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("BLPOP error: %v", err)
		}
		if r.Type != resp.Array || r.ListRESPData != nil {
			t.Fatalf("expected nil array, got %#v", r)
		}
		if elapsed < 200*time.Millisecond {
			t.Fatalf("expected timeout to take at least 200ms, took %v", elapsed)
		}
	})

	t.Run("blpop negative duration error", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("BLPOP", "key", "-1"), c); err == nil {
			t.Fatalf("expected error for negative duration")
		}
	})
}

// Additional XADD tests for edge cases
func TestHandleXADD_EdgeCases(t *testing.T) {
	t.Run("xadd rejects 0-0", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("XADD", "s", "0-0", "f", "v"), c); err == nil {
			t.Fatalf("expected error for ID 0-0")
		}
	})

	t.Run("xadd handles 0-* when stream is empty", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		r, err := h.Handle(cmd("XADD", "s", "0-*", "f", "v"), c)
		if err != nil {
			t.Fatalf("XADD error: %v", err)
		}
		if string(r.Data) != "0-1" {
			t.Fatalf("expected ID 0-1, got %q", string(r.Data))
		}
	})

	t.Run("xadd auto-generates sequence number", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// Add first entry
		r1, _ := h.Handle(cmd("XADD", "s", "100-*", "f", "v"), c)
		if string(r1.Data) != "100-0" {
			t.Fatalf("expected ID 100-0, got %q", string(r1.Data))
		}
		// Add second entry with same millis
		r2, _ := h.Handle(cmd("XADD", "s", "100-*", "f", "v2"), c)
		if string(r2.Data) != "100-1" {
			t.Fatalf("expected ID 100-1, got %q", string(r2.Data))
		}
	})

	t.Run("xadd rejects smaller ID", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s", "100-1", "f", "v"), c)
		if _, err := h.Handle(cmd("XADD", "s", "100-0", "f", "v2"), c); err == nil {
			t.Fatalf("expected error for smaller ID")
		}
	})

	t.Run("xadd wrong type error", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("SET", "str", "val"), c)
		if _, err := h.Handle(cmd("XADD", "str", "*", "f", "v"), c); err == nil {
			t.Fatalf("expected error when xadd on string key")
		}
	})

	t.Run("xadd invalid ID format", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("XADD", "s", "invalid", "f", "v"), c); err == nil {
			t.Fatalf("expected error for invalid ID format")
		}
	})
}

// Additional XRANGE tests
func TestHandleXRANGE_EdgeCases(t *testing.T) {
	t.Run("xrange with - and + symbols", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s", "100-0", "f", "v1"), c)
		h.Handle(cmd("XADD", "s", "200-0", "f", "v2"), c)
		r, err := h.Handle(cmd("XRANGE", "s", "-", "+"), c)
		if err != nil {
			t.Fatalf("XRANGE error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 2 {
			t.Fatalf("expected 2 entries, got %#v", r)
		}
	})

	t.Run("xrange partial ID format", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s", "100-0", "f", "v1"), c)
		h.Handle(cmd("XADD", "s", "100-1", "f", "v2"), c)
		h.Handle(cmd("XADD", "s", "200-0", "f", "v3"), c)
		// Use partial ID (millis only)
		r, err := h.Handle(cmd("XRANGE", "s", "100", "100"), c)
		if err != nil {
			t.Fatalf("XRANGE error: %v", err)
		}
		// Should match both 100-0 and 100-1
		if r.Type != resp.Array || len(r.ListRESPData) != 2 {
			t.Fatalf("expected 2 entries, got %#v", r)
		}
	})

	t.Run("xrange start > end returns empty", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s", "100-0", "f", "v"), c)
		r, err := h.Handle(cmd("XRANGE", "s", "200-0", "100-0"), c)
		if err != nil {
			t.Fatalf("XRANGE error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 0 {
			t.Fatalf("expected empty array, got %#v", r)
		}
	})

	t.Run("xrange on missing stream", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("XRANGE", "missing", "0-0", "1-0"), c); err == nil {
			t.Fatalf("expected error for missing stream")
		}
	})
}

// Additional XREAD tests
func TestHandleXREAD_EdgeCases(t *testing.T) {
	t.Run("xread with $ in blocking mode", func(t *testing.T) {
		h := setupTestHandler()
		xreadClient := createTestClient()
		defer xreadClient.CloseConn()

		// Create stream with existing entry using separate client
		setupClient := createTestClient()
		defer setupClient.CloseConn()
		h.Handle(cmd("XADD", "s", "100-0", "f", "old"), setupClient)

		// Start XREAD with $ (only new entries)
		outCh := make(chan *resp.RESPData, 1)
		go func() {
			r, _ := h.Handle(cmd("XREAD", "BLOCK", "500", "STREAMS", "s", "$"), xreadClient)
			outCh <- r
		}()
		time.Sleep(50 * time.Millisecond)

		// Add new entry
		addClient := createTestClient()
		defer addClient.CloseConn()
		h.Handle(cmd("XADD", "s", "200-0", "f", "new"), addClient)

		select {
		case r := <-outCh:
			if r == nil || r.Type != resp.Array {
				t.Fatalf("unexpected XREAD result: %#v", r)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for XREAD result")
		}
	})

	t.Run("xread $ in non-blocking mode errors", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s", "100-0", "f", "v"), c)
		if _, err := h.Handle(cmd("XREAD", "STREAMS", "s", "$"), c); err == nil {
			t.Fatalf("expected error for $ in non-blocking mode")
		}
	})

	t.Run("xread timeout returns nil", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s", "100-0", "f", "v"), c)
		start := time.Now()
		r, err := h.Handle(cmd("XREAD", "BLOCK", "200", "STREAMS", "s", "100-0"), c)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("XREAD error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 0 {
			t.Fatalf("expected empty array, got %#v", r)
		}
		if elapsed < 200*time.Millisecond {
			t.Fatalf("expected timeout to take at least 200ms, took %v", elapsed)
		}
	})

	t.Run("xread multiple streams", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		h.Handle(cmd("XADD", "s1", "100-0", "f", "v1"), c)
		h.Handle(cmd("XADD", "s2", "200-0", "f", "v2"), c)
		h.Handle(cmd("XADD", "s1", "150-0", "f", "v3"), c)

		r, err := h.Handle(cmd("XREAD", "STREAMS", "s1", "s2", "100-0", "100-0"), c)
		if err != nil {
			t.Fatalf("XREAD error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 2 {
			t.Fatalf("expected 2 stream results, got %#v", r)
		}
	})
}

// UNSUBSCRIBE tests
func TestHandleUNSUBSCRIBE(t *testing.T) {
	t.Run("unsubscribe removes subscription", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// Subscribe
		h.HandleSUBSCRIBE([]*resp.RESPData{resp.ConvertBulkStringToRESP("SUBSCRIBE"), resp.ConvertBulkStringToRESP("chan")}, c)

		// Unsubscribe
		r, err := h.HandleUNSUBSCRIBE([]*resp.RESPData{resp.ConvertBulkStringToRESP("UNSUBSCRIBE"), resp.ConvertBulkStringToRESP("chan")}, c)
		if err != nil {
			t.Fatalf("UNSUBSCRIBE error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 3 {
			t.Fatalf("expected 3-element array, got %#v", r)
		}
		if string(r.ListRESPData[0].Data) != "unsubscribe" {
			t.Fatalf("expected 'unsubscribe', got %q", string(r.ListRESPData[0].Data))
		}

		// Publish should now return 0 subscribers
		pubResp, _ := h.HandlePUBLISH([]*resp.RESPData{resp.ConvertBulkStringToRESP("PUBLISH"), resp.ConvertBulkStringToRESP("chan"), resp.ConvertBulkStringToRESP("msg")})
		pval, _ := pubResp.Int()
		if pval != 0 {
			t.Fatalf("expected 0 subscribers after unsubscribe, got %d", pval)
		}
	})
}

// COMMAND DOCS test
func TestHandleCOMMANDDOCS(t *testing.T) {
	t.Run("command docs returns empty array", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		r, err := h.Handle(cmd("COMMAND", "DOCS"), c)
		if err != nil {
			t.Fatalf("COMMAND DOCS error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 0 {
			t.Fatalf("expected empty array, got %#v", r)
		}
	})

	t.Run("command with wrong subcommand errors", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		if _, err := h.Handle(cmd("COMMAND", "INVALID"), c); err == nil {
			t.Fatalf("expected error for invalid subcommand")
		}
	})
}

// Test unknown command
func TestHandleUnknownCommand(t *testing.T) {
	h := setupTestHandler()
	c := createTestClient()
	defer c.CloseConn()

	if _, err := h.Handle(cmd("UNKNOWN"), c); err == nil {
		t.Fatalf("expected error for unknown command")
	}
}

// Test subscribe mode behavior
func TestSubscribeModeCommands(t *testing.T) {
	t.Run("subscribe mode allows limited commands", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// Put client in subscribe mode
		h.HandleSUBSCRIBE([]*resp.RESPData{resp.ConvertBulkStringToRESP("SUBSCRIBE"), resp.ConvertBulkStringToRESP("chan")}, c)

		// PING should work in subscribe mode (returns array format)
		r, err := h.Handle(cmd("PING"), c)
		if err != nil {
			t.Fatalf("PING in subscribe mode error: %v", err)
		}
		if r.Type != resp.Array || len(r.ListRESPData) != 2 {
			t.Fatalf("expected 2-element array for PING in subscribe mode, got %#v", r)
		}
	})

	t.Run("subscribe mode blocks other commands", func(t *testing.T) {
		h := setupTestHandler()
		c := createTestClient()
		defer c.CloseConn()

		// Put client in subscribe mode
		h.HandleSUBSCRIBE([]*resp.RESPData{resp.ConvertBulkStringToRESP("SUBSCRIBE"), resp.ConvertBulkStringToRESP("chan")}, c)

		if _, err := h.Handle(cmd("SET", "k", "v"), c); err == nil {
			t.Fatalf("expected error for SET in subscribe mode")
		}
	})
}

// Test CompareStreamIDs helper
func TestCompareStreamIDs(t *testing.T) {
	cases := []struct {
		idA  string
		idB  string
		want int
	}{
		{"100-0", "200-0", -1},
		{"200-0", "100-0", 1},
		{"100-0", "100-0", 0},
		{"100-0", "100-1", -1},
		{"100-1", "100-0", 1},
	}

	for _, c := range cases {
		t.Run(c.idA+" vs "+c.idB, func(t *testing.T) {
			got := storage.CompareStreamIDs(c.idA, c.idB)
			if got != c.want {
				t.Fatalf("CompareStreamIDs(%q, %q) = %d, want %d", c.idA, c.idB, got, c.want)
			}
		})
	}
}
