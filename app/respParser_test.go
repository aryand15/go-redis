package main

import (
	"testing"
)

func TestDecodeFromRESP_SimpleString(t *testing.T) {
	input := []byte("+OK\r\n")
	numRead, resp, success := DecodeFromRESP(input)

	if !success {
		t.Errorf("Expected success to be true, got false")
	}

	if numRead != len(input) {
		t.Errorf("Expected numRead to be %d, got %d", len(input), numRead)
	}

	if resp.Type != SimpleString {
		t.Errorf("Expected RESP type to be SimpleString, got %v", resp.Type)
	}

	if resp.String() != "OK" {
		t.Errorf("Expected RESP data to be 'OK', got '%s'", resp.String())
	}
}

func TestDecodeFromArray(t *testing.T) {
	input := []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	numRead, resp, success := DecodeFromRESP(input)

	if !success {
		t.Errorf("Expected success to be true, got false")
	}

	if numRead != len(input) {
		t.Errorf("Expected numRead to be %d, got %d", len(input), numRead)
	}

	if resp.Type != Array {
		t.Errorf("Expected RESP type to be Array, got %v", resp.Type)
	}

	if len(resp.NestedRESPData) != 2 {
		t.Errorf("Expected 2 nested RESP data elements, got %d", len(resp.NestedRESPData))
	}

	if resp.NestedRESPData[0].Type != BulkString || resp.NestedRESPData[0].String() != "GET" {
		t.Errorf("Expected first element to be BulkString 'GET', got type %v and data '%s'", resp.NestedRESPData[0].Type, resp.NestedRESPData[0].String())
	}

	if resp.NestedRESPData[1].Type != BulkString || resp.NestedRESPData[1].String() != "key" {
		t.Errorf("Expected second element to be BulkString 'key', got type %v and data '%s'", resp.NestedRESPData[1].Type, resp.NestedRESPData[1].String())
	}
}

func TestDecodeFromComplexArray(t *testing.T) {
	input := []byte("*7\r\n$5\r\nRPUSH\r\n$5\r\ngrape\r\n$6\r\norange\r\n$4\r\npear\r\n$10\r\nstrawberry\r\n$6\r\nbanana\r\n$9\r\npineapple\r\n")
	numRead, resp, success := DecodeFromRESP(input)

	if !success {
		t.Errorf("Expected success to be true, got false")
	}

	if numRead != len(input) {
		t.Errorf("Expected numRead to be %d, got %d", len(input), numRead)
	}

	if resp.Type != Array {
		t.Errorf("Expected RESP type to be Array, got %v", resp.Type)
	}

	if len(resp.NestedRESPData) != 7 {
		t.Errorf("Expected 3 nested RESP data elements, got %d", len(resp.NestedRESPData))
	}

	expectedValues := []string{"RPUSH", "grape", "orange", "pear", "strawberry", "banana", "pineapple"}
	for i, expected := range expectedValues {
		if resp.NestedRESPData[i].Type != BulkString || resp.NestedRESPData[i].String() != expected {
			t.Errorf("Expected element %d to be BulkString '%s', got type %v and data '%s'", i, expected, resp.NestedRESPData[i].Type, resp.NestedRESPData[i].String())
		}
	}
}

func TestDecodeEmptyArray(t *testing.T) {
	input := []byte("*0\r\n")
	numRead, resp, success := DecodeFromRESP(input)

	if !success {
		t.Errorf("Expected success to be true, got false")
	}

	if numRead != len(input) {
		t.Errorf("Expected numRead to be %d, got %d", len(input), numRead)
	}

	if resp.Type != Array {
		t.Errorf("Expected RESP type to be Array, got %v", resp.Type)
	}

	if len(resp.NestedRESPData) != 0 {
		t.Errorf("Expected 0 nested RESP data elements, got %d", len(resp.NestedRESPData))
	}
}

func TestEncodeEmptyArray(t *testing.T) {
	resp := &RESPData{
		Type:           Array,
		NestedRESPData: []*RESPData{},
	}

	encoded, success := EncodeToRESP(resp)
	if !success {
		t.Errorf("Expected no error during encoding, got an error")
	}

	expected := "*0\r\n"
	if string(encoded) != expected {
		t.Errorf("Expected encoded RESP to be '%s', got '%s'", expected, string(encoded))
	}
}