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
	Type         RESPType
	Data         []byte
	ListRESPData []*RESPData
}

func (r *RESPData) String() string {
	return string(r.Data)
}

func (r *RESPData) Int() (int, error) {
	return strconv.Atoi(r.String())
}

func CloneRESP(in *RESPData) *RESPData {
	out := &RESPData{Type: in.Type}
	if in.Data != nil {
		out.Data = append([]byte(nil), in.Data...)
	}
	if len(in.ListRESPData) > 0 {
		out.ListRESPData = make([]*RESPData, len(in.ListRESPData))
		for i, child := range in.ListRESPData {
			out.ListRESPData[i] = CloneRESP(child)
		}
	}
	return out
}

func ConvertBulkStringToRESP(s string) *RESPData {
	return &RESPData{
		Type: BulkString,
		Data: []byte(s),
	}
}

func ConvertIntToRESP(n int) *RESPData {
	return &RESPData{
		Type: Integer,
		Data: []byte(strconv.Itoa(n)),
	}
}

func convertListToRESP(arr []string) *RESPData {
	listResp := &RESPData{
		Type:         Array,
		ListRESPData: make([]*RESPData, len(arr)),
	}
	for i, s := range arr {
		listResp.ListRESPData[i] = ConvertBulkStringToRESP(s)
	}
	return listResp
}

func DecodeFromRESP(b []byte) (numRead int, resp *RESPData, success bool) {
	// Error: Byte array is empty
	if len(b) == 0 {
		return 0, nil, false
	}

	// Create new RESPData instance
	resp = &RESPData{}

	// Check if RESP type is valid
	resp.Type = RESPType(b[0])
	if !resp.Type.Valid() {
		return 0, nil, false
	}

	// Navigate to the next /r/n
	i := 1
	for ; !(b[i] == '\n' && b[i-1] == '\r'); i++ {
		// Error: Didn't reach an /r/n throughout the entire byte array
		if i == len(b) {
			return 0, nil, false
		}

		// Error: Missing \r before \n
		if b[i] == '\n' && b[i-1] != '\r' {
			return 0, nil, false
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

	// Error: Unable to parse as integer.
	if err != nil {
		return 0, nil, false
	}

	// Handle Integer type.
	// Integer format: :[<+|->]<value>\r\n
	if resp.Type == Integer {
		resp.Data = b[1 : i-1]
		return i + 1, resp, true
	}

	// Error: Type is Array and integer is negative.
	if length < 0 && resp.Type == Array {
		return 0, nil, false
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

		// Error: Data length is too short
		if (i + length + 2) > len(b) {
			return 0, nil, false
		}

		i += (length + 1)

		// Error: Didn't find an \r\n after reading appropriate amount of data
		if !(b[i] == '\n' && b[i-1] == '\r') {
			return 0, nil, false
		}

		resp.Data = b[j : j+length]
		return i + 1, resp, true
	}

	// Now for the absolute beast. Handle the Array type.
	// Array format: *<number-of-elements>\r\n<element-1>...<element-n>
	// Now "length" represents the number of elements in the array
	resp.ListRESPData = make([]*RESPData, length)
	for idx := 0; idx < length; idx++ {
		// Errror: Not enough array elements were able to be read.
		if i == len(b) {
			return 0, nil, false
		}

		rread, rresp, rsuccess := DecodeFromRESP(b[i:])

		// Error: One of the array RESP elements was unsuccessfully parsed.
		if !rsuccess {
			return 0, nil, false
		}

		// Add RESPData to list of array elements
		resp.ListRESPData[idx] = rresp
		// Update number of read bytes
		i += rread
	}

	return i, resp, true

}

func EncodeToRESP(r *RESPData) (b []byte, success bool) {
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
		if r.Data == nil {
			return []byte("$-1\r\n"), true
		}

		strlen := len(r.Data)
		s := "$" + strconv.Itoa(strlen) + "\r\n" + string(r.Data) + "\r\n"
		return []byte(s), true
	case Array:
		if r.ListRESPData == nil {
			return []byte("*-1\r\n"), true
		}
		arrlen := len(r.ListRESPData)
		s := "*" + strconv.Itoa(arrlen) + "\r\n"
		for _, rr := range r.ListRESPData {
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
