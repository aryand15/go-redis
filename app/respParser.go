package main

import (
	"strconv"
)

type RESPType byte

const (
	SimpleString = '+'
	SimpleError  = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

func (t RESPType) Valid() bool {
	switch t {
	case SimpleString, SimpleError, Integer, BulkString, Array:
		return true
	default:
		return false
	}
}

type RESPData struct {
	Type           RESPType
	Data           []byte
	NestedRESPData []RESPData
}

func (r RESPData) String() string {
	return string(r.Data)
}

func DecodeFromRESP(b []byte) (numRead int, resp RESPData, success bool) {
	// Error: Byte array is empty
	if len(b) == 0 {
		return 0, RESPData{}, false
	}

	// Error: Converting first byte to a valid RESP type fails
	resp.Type = RESPType(b[0])
	if !resp.Type.Valid() {
		return 0, RESPData{}, false
	}

	// Navigate to the next /r/n
	i := 1
	for ; !(b[i] == '\n' && b[i-1] == '\r'); i++ {
		// Didn't reach an /r/n throughout the entire byte array
		if i == len(b) {
			return 0, RESPData{}, false
		}

		// Missing \r before \n
		if b[i] == '\n' && b[i-1] != '\r' {
			return 0, RESPData{}, false
		}
	}

	// Handle Simple String and Simple Error types.
	// Simple String format: +OK\r\n
	// SimpleError format: -Error message\r\n
	if resp.Type == SimpleString || resp.Type == SimpleError {
		resp.Data = b[1 : i-1]
		return i + 1, resp, true
	}

	// Attempt to convert captured data to integer.
	length, err := strconv.Atoi(string(b[1 : i-1]))

	// Unable to parse as integer.
	if err != nil {
		return 0, RESPData{}, false
	}

	// Handle Integer type.
	// Integer format: :[<+|->]<value>\r\n
	if resp.Type == Integer {
		resp.Data = b[1 : i-1]
		return i + 1, resp, true
	}

	// Type is Array and integer is negative.
	if length < 0 && resp.Type == Array {
		return 0, RESPData{}, false
	}

	// Type is Bulk String and integer is negative (null bulk string).
	if length < 0 && resp.Type == BulkString {
		resp.Data = nil
		return i + 1, resp, true
	}

	i += 1
	// Handle Bulk String type
	// Bulk String format: $<length>\r\n<data>\r\n
	if resp.Type == BulkString {
		j := i

		// Data length is too short
		if (i + length + 2) > len(b) {
			return 0, RESPData{}, false
		}

		i += (length + 1)

		// Didn't find an \r\n after reading appropriate amount of data
		if !(b[i] == '\n' && b[i-1] == '\r') {
			return 0, RESPData{}, false
		}

		resp.Data = b[j : j+length]
		return i + 1, resp, true
	}

	// Now for the absolute beast. Handle the Array type.
	// Array format: *<number-of-elements>\r\n<element-1>...<element-n>
	// Now "length" represents the number of elements in the array
	resp.NestedRESPData = make([]RESPData, length)
	for idx := 0; idx < length; idx++ {
		// Not enough array elements were able to be read.
		if i == len(b) {
			return 0, RESPData{}, false
		}

		rread, rresp, rsuccess := DecodeFromRESP(b[i:])

		// One of the array RESP elements was unsuccessfully parsed.
		if !rsuccess {
			return 0, RESPData{}, false
		}

		// Add RESPData to list of array elements
		resp.NestedRESPData[idx] = rresp
		// Update number of read bytes
		i += rread
	}

	return i, resp, true

}

func EncodeToRESP(r RESPData) (b []byte, success bool) {
	// This is assuming that the RESPData passes all validation.

	switch r.Type {
	case SimpleString:
		s := "+" + string(r.Data) + "\r\n"
		return []byte(s), true
	case SimpleError:
		s := "-" + string(r.Data) + "\r\n"
		return []byte(s), true
	case Integer:
		s := ":" + string(r.Data) + "\r\n"
		return []byte(s), true
	case BulkString:
		strlen := len(r.Data)
		s := "$" + strconv.Itoa(strlen) + "\r\n" + string(r.Data) + "\r\n"
		return []byte(s), true
	case Array:
		arrlen := len(r.NestedRESPData)
		s := "*" + strconv.Itoa(arrlen) + "\r\n"
		for _, rr := range r.NestedRESPData {
			res, suc := EncodeToRESP(rr)
			if !suc {
				return nil, false
			}
			s = s + string(res)
		}
		return []byte(s), true
	default:
		return nil, false
		
	}
}
