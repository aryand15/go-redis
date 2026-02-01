package resp

import (
	"testing"
)

func TestDecodeFromRESP_SimpleString(t *testing.T) {
	input := []byte("+OK\r\n")
	resp, err := DecodeFromRESP(input)

	if err != nil {
		t.Errorf("Expected no error, got: %s", err.Error())
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
	resp, err := DecodeFromRESP(input)

	if err != nil {
		t.Errorf("Expected no error, got: %s", err.Error())
	}

	if resp.Type != Array {
		t.Errorf("Expected RESP type to be Array, got %v", resp.Type)
	}

	if len(resp.ListRESPData) != 2 {
		t.Errorf("Expected 2 nested RESP data elements, got %d", len(resp.ListRESPData))
	}

	if resp.ListRESPData[0].Type != BulkString || resp.ListRESPData[0].String() != "GET" {
		t.Errorf("Expected first element to be BulkString 'GET', got type %v and data '%s'", resp.ListRESPData[0].Type, resp.ListRESPData[0].String())
	}

	if resp.ListRESPData[1].Type != BulkString || resp.ListRESPData[1].String() != "key" {
		t.Errorf("Expected second element to be BulkString 'key', got type %v and data '%s'", resp.ListRESPData[1].Type, resp.ListRESPData[1].String())
	}
}

func TestDecodeFromComplexArray(t *testing.T) {
	input := []byte("*7\r\n$5\r\nRPUSH\r\n$5\r\ngrape\r\n$6\r\norange\r\n$4\r\npear\r\n$10\r\nstrawberry\r\n$6\r\nbanana\r\n$9\r\npineapple\r\n")
	resp, err := DecodeFromRESP(input)

	if err != nil {
		t.Errorf("Expected no error, got: %s", err.Error())
	}

	if resp.Type != Array {
		t.Errorf("Expected RESP type to be Array, got %v", resp.Type)
	}

	if len(resp.ListRESPData) != 7 {
		t.Errorf("Expected 3 nested RESP data elements, got %d", len(resp.ListRESPData))
	}

	expectedValues := []string{"RPUSH", "grape", "orange", "pear", "strawberry", "banana", "pineapple"}
	for i, expected := range expectedValues {
		if resp.ListRESPData[i].Type != BulkString || resp.ListRESPData[i].String() != expected {
			t.Errorf("Expected element %d to be BulkString '%s', got type %v and data '%s'", i, expected, resp.ListRESPData[i].Type, resp.ListRESPData[i].String())
		}
	}
}

func TestDecodeEmptyArray(t *testing.T) {
	input := []byte("*0\r\n")
	resp, err := DecodeFromRESP(input)

	if err != nil {
		t.Errorf("Expected no error, got: %s", err.Error())
	}

	if resp.Type != Array {
		t.Errorf("Expected RESP type to be Array, got %v", resp.Type)
	}

	if len(resp.ListRESPData) != 0 {
		t.Errorf("Expected 0 nested RESP data elements, got %d", len(resp.ListRESPData))
	}
}

func TestEncodeEmptyArray(t *testing.T) {
	resp := &RESPData{
		Type:         Array,
		ListRESPData: []*RESPData{},
	}

	encoded, success := resp.EncodeToRESP()
	if !success {
		t.Errorf("Expected no error during encoding, got an error")
	}

	expected := "*0\r\n"
	if string(encoded) != expected {
		t.Errorf("Expected encoded RESP to be '%s', got '%s'", expected, string(encoded))
	}
}

// Additional comprehensive tests

func TestDecodeFromRESP_SimpleError(t *testing.T) {
	input := []byte("-ERR unknown command\r\n")
	resp, err := DecodeFromRESP(input)

	if err != nil {
		t.Fatal("Expected success to be true")
	}

	if resp.Type != SimpleError {
		t.Errorf("Expected SimpleError, got %v", resp.Type)
	}

	if resp.String() != "ERR unknown command" {
		t.Errorf("Expected 'ERR unknown command', got '%s'", resp.String())
	}
}

func TestDecodeFromRESP_Integer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"positive", ":42\r\n", "42"},
		{"negative", ":-100\r\n", "-100"},
		{"zero", ":0\r\n", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := DecodeFromRESP([]byte(tt.input))
			if err != nil {
				t.Fatal("Expected success to be true")
			}
			if resp.Type != Integer {
				t.Errorf("Expected Integer type")
			}
			if resp.String() != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, resp.String())
			}
		})
	}
}

func TestDecodeFromRESP_BulkString(t *testing.T) {
	t.Run("normal bulk string", func(t *testing.T) {
		input := []byte("$5\r\nhello\r\n")
		resp, err := DecodeFromRESP(input)

		if err != nil {
			t.Fatalf("Expected success, got: %s", err.Error())
		}
		if resp.Type != BulkString {
			t.Errorf("Expected BulkString")
		}
		if resp.String() != "hello" {
			t.Errorf("Expected 'hello', got '%s'", resp.String())
		}
	})

	t.Run("null bulk string", func(t *testing.T) {
		input := []byte("$-1\r\n")
		resp, err := DecodeFromRESP(input)

		if err != nil {
			t.Fatalf("Expected success, got: %s", err.Error())
		}
		if resp.Type != BulkString {
			t.Errorf("Expected BulkString")
		}
		if resp.Data != nil {
			t.Errorf("Expected nil data for null bulk string")
		}
	})

	t.Run("empty bulk string", func(t *testing.T) {
		input := []byte("$0\r\n\r\n")
		resp, err := DecodeFromRESP(input)

		if err != nil {
			t.Fatalf("Expected success, got: %s", err.Error())
		}
		if resp.String() != "" {
			t.Errorf("Expected empty string, got '%s'", resp.String())
		}
	})
}

func TestDecodeFromRESP_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"empty input", []byte{}},
		{"invalid type", []byte("@invalid\r\n")},
		{"missing crlf", []byte("$5hello")},
		{"incomplete bulk string", []byte("$5\r\nhel")},
		{"wrong bulk string length", []byte("$10\r\nhello\r\n")},
		{"invalid array length", []byte("*abc\r\n")},
		{"incomplete array", []byte("*2\r\n$3\r\nGET\r\n")},
		{"simple string with binary char", []byte("+hihi\r\r\n")},
		{"simple string with extra crlf", []byte("+hihi\r\n\r\n")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeFromRESP(tt.input)
			if err == nil {
				t.Errorf("Expected failure for %s", tt.name)
			}
		})
	}
}

func TestEncodeToRESP_SimpleString(t *testing.T) {
	resp := ConvertSimpleStringToRESP("OK")
	encoded, success := resp.EncodeToRESP()

	if !success {
		t.Fatal("Expected success")
	}

	expected := "+OK\r\n"
	if string(encoded) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(encoded))
	}
}

func TestEncodeToRESP_SimpleError(t *testing.T) {
	resp := ConvertSimpleErrorToRESP("ERR test error")
	encoded, success := resp.EncodeToRESP()

	if !success {
		t.Fatal("Expected success")
	}

	expected := "-ERR test error\r\n"
	if string(encoded) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(encoded))
	}
}

func TestEncodeToRESP_Integer(t *testing.T) {
	resp := ConvertIntToRESP(42)
	encoded, success := resp.EncodeToRESP()

	if !success {
		t.Fatal("Expected success")
	}

	expected := ":42\r\n"
	if string(encoded) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(encoded))
	}
}

func TestEncodeToRESP_BulkString(t *testing.T) {
	t.Run("normal bulk string", func(t *testing.T) {
		resp := ConvertBulkStringToRESP("hello")
		encoded, success := resp.EncodeToRESP()

		if !success {
			t.Fatal("Expected success")
		}

		expected := "$5\r\nhello\r\n"
		if string(encoded) != expected {
			t.Errorf("Expected '%s', got '%s'", expected, string(encoded))
		}
	})

	t.Run("null bulk string", func(t *testing.T) {
		resp := &RESPData{Type: BulkString, Data: nil}
		encoded, success := resp.EncodeToRESP()

		if !success {
			t.Fatal("Expected success")
		}

		if string(encoded) != string(RespNullString) {
			t.Errorf("Expected null bulk string")
		}
	})
}

func TestEncodeToRESP_Array(t *testing.T) {
	t.Run("array of bulk strings", func(t *testing.T) {
		resp := ConvertListToRESP([]string{"GET", "key"})
		encoded, success := resp.EncodeToRESP()

		if !success {
			t.Fatal("Expected success")
		}

		expected := "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
		if string(encoded) != expected {
			t.Errorf("Expected '%s', got '%s'", expected, string(encoded))
		}
	})

	t.Run("null array", func(t *testing.T) {
		resp := &RESPData{Type: Array, ListRESPData: nil}
		encoded, success := resp.EncodeToRESP()

		if !success {
			t.Fatal("Expected success")
		}

		if string(encoded) != string(RespNullArr) {
			t.Errorf("Expected null array")
		}
	})
}

func TestRoundTrip(t *testing.T) {
	tests := []string{
		"+OK\r\n",
		"-ERR test\r\n",
		":123\r\n",
		"$5\r\nhello\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
	}

	for _, original := range tests {
		t.Run(original, func(t *testing.T) {
			// Decode
			resp, err := DecodeFromRESP([]byte(original))
			if err != nil {
				t.Fatalf("Decode failed: %s", err.Error())
			}

			// Encode
			encoded, success := resp.EncodeToRESP()
			if !success {
				t.Fatal("Encode failed")
			}

			if string(encoded) != original {
				t.Errorf("Round trip failed: expected '%s', got '%s'", original, string(encoded))
			}
		})
	}
}

func TestCloneRESP(t *testing.T) {
	t.Run("clone simple string", func(t *testing.T) {
		original := ConvertSimpleStringToRESP("test")
		cloned := CloneRESP(original)

		if cloned == original {
			t.Error("Clone should create new instance")
		}

		if string(cloned.Data) != string(original.Data) {
			t.Error("Cloned data should match")
		}

		// Modify clone shouldn't affect original
		cloned.Data = []byte("modified")
		if string(original.Data) == "modified" {
			t.Error("Modifying clone affected original")
		}
	})

	t.Run("clone array", func(t *testing.T) {
		original := ConvertListToRESP([]string{"a", "b", "c"})
		cloned := CloneRESP(original)

		if cloned == original {
			t.Error("Clone should create new instance")
		}

		if len(cloned.ListRESPData) != len(original.ListRESPData) {
			t.Error("Cloned array length should match")
		}

		// Modify clone shouldn't affect original
		cloned.ListRESPData[0].Data = []byte("modified")
		if string(original.ListRESPData[0].Data) == "modified" {
			t.Error("Modifying clone affected original")
		}
	})
}

func TestRESPType_Valid(t *testing.T) {
	validTypes := []RESPType{SimpleString, SimpleError, Integer, BulkString, Array}
	for _, typ := range validTypes {
		if !typ.Valid() {
			t.Errorf("Type %v should be valid", typ)
		}
	}

	invalidTypes := []RESPType{'@', '#', '!'}
	for _, typ := range invalidTypes {
		if typ.Valid() {
			t.Errorf("Type %v should be invalid", typ)
		}
	}
}

func TestRESPData_Int(t *testing.T) {
	t.Run("valid integer", func(t *testing.T) {
		resp := ConvertIntToRESP(42)
		val, err := resp.Int()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	})

	t.Run("invalid integer", func(t *testing.T) {
		resp := ConvertBulkStringToRESP("not a number")
		_, err := resp.Int()
		if err == nil {
			t.Error("Expected error for non-integer string")
		}
	})
}
