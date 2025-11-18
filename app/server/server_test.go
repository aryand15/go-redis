package server

import (
	"bytes"
	"github.com/aryand15/go-redis/app/commands"
	"github.com/aryand15/go-redis/app/storage"
	"github.com/aryand15/go-redis/resp"
	"net"
	"strings"
	"testing"
	"time"
)

// Helper function to create a test command handler
func setupTestHandler() *commands.CommandHandler {
	db := storage.NewDB()
	return commands.NewCommandHandler(db)
}

// Helper function to encode a command as RESP
func encodeCommand(args ...string) []byte {
	respData := resp.ConvertListToRESP(args)
	encoded, _ := respData.EncodeToRESP()
	return encoded
}

// Helper function to read and decode a RESP response
func readResponse(conn net.Conn) (*resp.RESPData, error) {
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	_, respData, success := resp.DecodeFromRESP(buf[:n])
	if !success {
		return nil, err
	}
	return respData, nil
}

// Test basic connection handling
func TestHandleConn_BasicCommands(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Test PING command
	client.Write(encodeCommand("PING"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading PING response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String() != "PONG" {
		t.Errorf("Expected PONG, got %v", response.String())
	}

	// Test ECHO command
	client.Write(encodeCommand("ECHO", "hello"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading ECHO response: %v", err)
	}
	if response.Type != resp.BulkString || response.String() != "hello" {
		t.Errorf("Expected 'hello', got %v", response.String())
	}

	// Test SET and GET commands
	client.Write(encodeCommand("SET", "key1", "value1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading SET response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String() != "OK" {
		t.Errorf("Expected OK, got %v", response.String())
	}

	client.Write(encodeCommand("GET", "key1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading GET response: %v", err)
	}
	if response.Type != resp.BulkString || response.String() != "value1" {
		t.Errorf("Expected 'value1', got %v", response.String())
	}
}

// Test transaction mode
func TestHandleConn_TransactionMode(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Start transaction
	client.Write(encodeCommand("MULTI"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading MULTI response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String() != "OK" {
		t.Errorf("Expected OK for MULTI, got %v", response.String())
	}

	// Queue commands
	client.Write(encodeCommand("SET", "tx1", "val1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading queued SET response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String() != "QUEUED" {
		t.Errorf("Expected QUEUED, got %v", response.String())
	}

	client.Write(encodeCommand("SET", "tx2", "val2"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading second queued SET response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String() != "QUEUED" {
		t.Errorf("Expected QUEUED, got %v", response.String())
	}

	// Execute transaction
	client.Write(encodeCommand("EXEC"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading EXEC response: %v", err)
	}
	if response.Type != resp.Array || len(response.ListRESPData) != 2 {
		t.Errorf("Expected array with 2 results, got %v", response)
	}

	// Verify commands were executed
	client.Write(encodeCommand("GET", "tx1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading GET response: %v", err)
	}
	if response.String() != "val1" {
		t.Errorf("Expected 'val1', got %v", response.String())
	}
}

// Test DISCARD in transaction mode
func TestHandleConn_TransactionDiscard(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Start transaction
	client.Write(encodeCommand("MULTI"))
	readResponse(client) // Consume MULTI response

	// Queue commands
	client.Write(encodeCommand("SET", "discard1", "val1"))
	readResponse(client) // Consume QUEUED response

	client.Write(encodeCommand("SET", "discard2", "val2"))
	readResponse(client) // Consume QUEUED response

	// Discard transaction
	client.Write(encodeCommand("DISCARD"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading DISCARD response: %v", err)
	}
	if response.Type != resp.SimpleString || response.String() != "OK" {
		t.Errorf("Expected OK for DISCARD, got %v", response.String())
	}

	// Verify commands were not executed
	client.Write(encodeCommand("GET", "discard1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading GET response: %v", err)
	}
	if response.Data != nil {
		t.Errorf("Expected nil (key not set), got %v", response.String())
	}
}

// Test subscribe mode
func TestHandleConn_SubscribeMode(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Subscribe to a channel
	client.Write(encodeCommand("SUBSCRIBE", "channel1"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading SUBSCRIBE response: %v", err)
	}

	// Response should be an array: ["subscribe", "channel1", 1]
	if response.Type != resp.Array || len(response.ListRESPData) != 3 {
		t.Fatalf("Expected array with 3 elements, got %v", response)
	}
	if response.ListRESPData[0].String() != "subscribe" {
		t.Errorf("Expected 'subscribe', got %v", response.ListRESPData[0].String())
	}
	if response.ListRESPData[1].String() != "channel1" {
		t.Errorf("Expected 'channel1', got %v", response.ListRESPData[1].String())
	}

	// Verify the subscriber was registered in the database
	handler.GetDB().Lock()
	_, hasReceiver := handler.GetDB().GetReceiver(server)
	handler.GetDB().Unlock()
	if !hasReceiver {
		t.Error("Expected subscriber to be registered")
	}
}

// Test UNSUBSCRIBE in subscribe mode
func TestHandleConn_UnsubscribeMode(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	
	go handleConn(server, handler)
	
	// Subscribe to two channels
	client.Write(encodeCommand("SUBSCRIBE", "ch1"))
	readResponse(client) // Consume first subscribe response
	
	client.Write(encodeCommand("SUBSCRIBE", "ch2"))
	readResponse(client) // Consume second subscribe response
	
	// Now we're in subscribe mode, try to unsubscribe from one channel
	client.Write(encodeCommand("UNSUBSCRIBE", "ch1"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading UNSUBSCRIBE response: %v", err)
	}
	
	// Response should be an array: ["unsubscribe", "ch1", <remaining count>]
	if response.Type != resp.Array {
		t.Fatalf("Expected Array response type, got %v", response.Type)
	}
	if len(response.ListRESPData) != 3 {
		t.Fatalf("Expected array with 3 elements, got %d elements", len(response.ListRESPData))
	}
	if response.ListRESPData[0].String() != "unsubscribe" {
		t.Errorf("Expected 'unsubscribe', got %v", response.ListRESPData[0].String())
	}
	if response.ListRESPData[1].String() != "ch1" {
		t.Errorf("Expected channel 'ch1', got %v", response.ListRESPData[1].String())
	}
}

// Test QUIT command in subscribe mode
func TestHandleConn_QuitInSubscribeMode(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Subscribe to a channel
	client.Write(encodeCommand("SUBSCRIBE", "channel1"))
	readResponse(client) // Consume subscribe response

	// Send QUIT to exit subscribe mode
	client.Write(encodeCommand("QUIT"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading QUIT response: %v", err)
	}

	// QUIT should return some response (could be SimpleString or Array)
	// The key is that it doesn't error
	if response == nil {
		t.Error("Expected non-nil response to QUIT")
	}

	// After quitting subscribe mode, should be able to run normal commands
	client.Write(encodeCommand("PING"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading PING after QUIT: %v", err)
	}
	if response.String() != "PONG" {
		t.Errorf("Expected PONG after exiting subscribe mode, got %v", response.String())
	}
}

// Test error handling for invalid RESP
func TestHandleConn_InvalidRESP(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan bool)
	go func() {
		handleConn(server, handler)
		done <- true
	}()

	// Send invalid RESP (not an array)
	invalidResp := resp.ConvertSimpleStringToRESP("NOT_AN_ARRAY")
	encoded, _ := invalidResp.EncodeToRESP()
	client.Write(encoded)

	// The server should close the connection
	select {
	case <-done:
		// Connection was closed as expected
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected connection to close after invalid RESP")
	}
}

// Test error handling for empty array
func TestHandleConn_EmptyArray(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan bool)
	go func() {
		handleConn(server, handler)
		done <- true
	}()

	// Send empty array
	emptyArray := &resp.RESPData{Type: resp.Array, ListRESPData: []*resp.RESPData{}}
	encoded, _ := emptyArray.EncodeToRESP()
	client.Write(encoded)

	// The server should close the connection
	select {
	case <-done:
		// Connection was closed as expected
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected connection to close after empty array")
	}
}

// Test cleanup on disconnect
func TestHandleConn_Cleanup(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()

	go handleConn(server, handler)

	// Subscribe to channels
	client.Write(encodeCommand("SUBSCRIBE", "cleanup_channel"))
	readResponse(client) // Consume subscribe response

	// Verify subscriber was added
	handler.GetDB().Lock()
	_, hasReceiver := handler.GetDB().GetReceiver(server)
	if !hasReceiver {
		handler.GetDB().Unlock()
		t.Fatal("Expected receiver to be registered")
	}
	handler.GetDB().Unlock()

	// Close the client connection
	client.Close()

	// Give the cleanup defer a moment to run
	time.Sleep(100 * time.Millisecond)

	// Verify cleanup happened
	handler.GetDB().Lock()
	_, hasReceiver = handler.GetDB().GetReceiver(server)
	if hasReceiver {
		handler.GetDB().Unlock()
		t.Error("Expected receiver to be cleaned up after disconnect")
	}

	// Verify subscriber was removed from publishers
	if pubs, ok := handler.GetDB().GetPublishers(server); ok {
		handler.GetDB().Unlock()
		t.Errorf("Expected publishers to be cleaned up, found %d", pubs.Length())
	} else {
		handler.GetDB().Unlock()
	}
}

// Test concurrent connections
func TestHandleConn_ConcurrentConnections(t *testing.T) {
	handler := setupTestHandler()

	// Create multiple concurrent connections
	numClients := 5
	clients := make([]net.Conn, numClients)
	servers := make([]net.Conn, numClients)

	for i := 0; i < numClients; i++ {
		client, server := net.Pipe()
		clients[i] = client
		servers[i] = server
		go handleConn(server, handler)
	}

	defer func() {
		for i := 0; i < numClients; i++ {
			clients[i].Close()
			servers[i].Close()
		}
	}()

	// Each client sends a SET command with unique key
	for i := 0; i < numClients; i++ {
		key := "concurrent_key_" + string(rune('0'+i))
		value := "value_" + string(rune('0'+i))
		clients[i].Write(encodeCommand("SET", key, value))
	}

	// Read responses
	for i := 0; i < numClients; i++ {
		response, err := readResponse(clients[i])
		if err != nil {
			t.Fatalf("Client %d error: %v", i, err)
		}
		if response.String() != "OK" {
			t.Errorf("Client %d expected OK, got %v", i, response.String())
		}
	}

	// Verify all keys were set correctly
	for i := 0; i < numClients; i++ {
		key := "concurrent_key_" + string(rune('0'+i))
		expectedValue := "value_" + string(rune('0'+i))

		clients[i].Write(encodeCommand("GET", key))
		response, err := readResponse(clients[i])
		if err != nil {
			t.Fatalf("Client %d GET error: %v", i, err)
		}
		if response.String() != expectedValue {
			t.Errorf("Client %d expected %v, got %v", i, expectedValue, response.String())
		}
	}
}

// Test error responses
func TestHandleConn_ErrorResponses(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Test unknown command
	client.Write(encodeCommand("UNKNOWN", "arg1"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}
	if response.Type != resp.SimpleError {
		t.Errorf("Expected SimpleError, got %v", response.Type)
	}
	if !strings.Contains(response.String(), "unknown command") {
		t.Errorf("Expected error message about unknown command, got %v", response.String())
	}

	// Test wrong number of arguments
	client.Write(encodeCommand("SET", "key1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error reading response: %v", err)
	}
	if response.Type != resp.SimpleError {
		t.Errorf("Expected SimpleError, got %v", response.Type)
	}
}

// Test switching between modes
func TestHandleConn_ModeSwitching(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Normal mode - SET command
	client.Write(encodeCommand("SET", "k1", "v1"))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error in normal mode: %v", err)
	}
	if response.String() != "OK" {
		t.Errorf("Expected OK, got %v", response.String())
	}

	// Enter transaction mode
	client.Write(encodeCommand("MULTI"))
	readResponse(client)

	client.Write(encodeCommand("SET", "k2", "v2"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error in transaction mode: %v", err)
	}
	if response.String() != "QUEUED" {
		t.Errorf("Expected QUEUED, got %v", response.String())
	}

	// Exit transaction mode with DISCARD
	client.Write(encodeCommand("DISCARD"))
	readResponse(client)

	// Back to normal mode
	client.Write(encodeCommand("GET", "k1"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error after exiting transaction: %v", err)
	}
	if response.String() != "v1" {
		t.Errorf("Expected 'v1', got %v", response.String())
	}

	// Enter subscribe mode
	client.Write(encodeCommand("SUBSCRIBE", "ch1"))
	readResponse(client)

	// Exit subscribe mode with QUIT
	client.Write(encodeCommand("QUIT"))
	readResponse(client)

	// Back to normal mode again
	client.Write(encodeCommand("PING"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error after exiting subscribe mode: %v", err)
	}
	if response.String() != "PONG" {
		t.Errorf("Expected PONG, got %v", response.String())
	}
}

// Test connection read timeout/closure
func TestHandleConn_ConnectionClosure(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()

	done := make(chan bool)
	go func() {
		handleConn(server, handler)
		done <- true
	}()

	// Send a command
	client.Write(encodeCommand("PING"))
	readResponse(client)

	// Close client connection
	client.Close()

	// Wait for handleConn to finish
	select {
	case <-done:
		// Connection handler exited as expected
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected handleConn to exit after connection closure")
	}
}

// Test multiple sequential commands
func TestHandleConn_SequentialCommands(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	commands := []struct {
		cmd      []string
		expected string
		respType resp.RESPType
	}{
		{[]string{"PING"}, "PONG", resp.SimpleString},
		{[]string{"SET", "seq1", "val1"}, "OK", resp.SimpleString},
		{[]string{"GET", "seq1"}, "val1", resp.BulkString},
		{[]string{"INCR", "counter"}, "1", resp.Integer},
		{[]string{"INCR", "counter"}, "2", resp.Integer},
		{[]string{"GET", "counter"}, "2", resp.BulkString},
	}

	for i, tc := range commands {
		client.Write(encodeCommand(tc.cmd...))
		response, err := readResponse(client)
		if err != nil {
			t.Fatalf("Command %d error: %v", i, err)
		}
		if response.Type != tc.respType {
			t.Errorf("Command %d expected type %v, got %v", i, tc.respType, response.Type)
		}
		if response.String() != tc.expected {
			t.Errorf("Command %d expected %v, got %v", i, tc.expected, response.String())
		}
	}
}

// Test buffer handling with moderately sized data
func TestHandleConn_ModerateData(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go handleConn(server, handler)

	// Create a moderate value (avoid hitting buffer limits)
	moderateValue := strings.Repeat("a", 512)

	client.Write(encodeCommand("SET", "moderate_key", moderateValue))
	response, err := readResponse(client)
	if err != nil {
		t.Fatalf("Error with moderate SET: %v", err)
	}
	if response.String() != "OK" {
		t.Errorf("Expected OK, got %v", response.String())
	}

	client.Write(encodeCommand("GET", "moderate_key"))
	response, err = readResponse(client)
	if err != nil {
		t.Fatalf("Error with moderate GET: %v", err)
	}
	if response.String() != moderateValue {
		t.Errorf("Moderate value mismatch: expected length %d, got %d", len(moderateValue), len(response.String()))
	}
}

// Test malformed commands in different modes
func TestHandleConn_MalformedCommands(t *testing.T) {
	handler := setupTestHandler()
	client, server := net.Pipe()
	defer client.Close()

	done := make(chan bool)
	go func() {
		handleConn(server, handler)
		done <- true
	}()

	// Send malformed data (not valid RESP)
	client.Write([]byte("this is not RESP\r\n"))

	// Connection should be closed
	select {
	case <-done:
		// Expected behavior
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected connection to close on malformed data")
	}
}

// Test RESP encoding edge cases
func TestEncodeCommand(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected string
	}{
		{"single arg", []string{"PING"}, "*1\r\n$4\r\nPING\r\n"},
		{"two args", []string{"ECHO", "hi"}, "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"},
		{"empty string arg", []string{"SET", "k", ""}, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeCommand(tt.args...)
			if !bytes.Equal(encoded, []byte(tt.expected)) {
				t.Errorf("Expected %q, got %q", tt.expected, string(encoded))
			}
		})
	}
}
