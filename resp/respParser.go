package resp

import (
	"strconv"
	"fmt"
)

type RESPType byte

const (
	SimpleString = '+'
	SimpleError  = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// Common RESP responses
var (
	RespOK         = []byte("+OK\r\n")
	RespPong       = []byte("+PONG\r\n")
	RespNullString = []byte("$-1\r\n")
	RespEmptyList   = []byte("*0\r\n")
	RespNullArr    = []byte("*-1\r\n")
	RespNoneString = []byte("+none\r\n")
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

func ConvertIntToRESP(n int64) *RESPData {
	return &RESPData{
		Type: Integer,
		Data: []byte(strconv.FormatInt(n, 10)),
	}
}

func ConvertListToRESP(arr []string) *RESPData {
	listResp := &RESPData{
		Type:         Array,
		ListRESPData: make([]*RESPData, len(arr)),
	}
	for i, s := range arr {
		listResp.ListRESPData[i] = ConvertBulkStringToRESP(s)
	}
	return listResp
}

func ConvertSimpleErrorToRESP(s string) *RESPData {
	return &RESPData{
		Type: SimpleError,
		Data: []byte(s),
	}
}

func ConvertSimpleStringToRESP(s string) *RESPData {
	return &RESPData{
		Type: SimpleString,
		Data: []byte(s),
	}
}

func DecodeFromRESP(b []byte) (resp *RESPData, err error) {
	_, resp, err = decodeFromRESP(b, 0)
	return resp, err
}

func decodeFromRESP(b []byte, start int) (numRead int, resp *RESPData, err error) {

	// Error: Byte array length is not sufficient
	// Smallest possible string is: "+\r\n"
	msgLength := len(b) - start	// make sure to account for start index offset
	if msgLength < 3 {
		return 0, nil, fmt.Errorf("insufficient length: %s is of length %d but should be at least 3", b[start:], msgLength)
	}

	// Create new RESPData instance
	resp = &RESPData{}

	// Check if RESP type is valid
	resp.Type = RESPType(b[start])
	if !resp.Type.Valid() {
		return 0, nil, fmt.Errorf("invalid RESP type at index %d: %b", start, resp.Type)
	}

	// There should always be at least one CRLF (\r\n) in the byte array.
	// Try to navigate to the next CRLF
	i := start + 2
	for ; !(b[i] == '\n' && b[i-1] == '\r'); i++ {
		// Error: Didn't reach a CRLF throughout the entire byte array
		if i == len(b)-1 {
			return 0, nil, fmt.Errorf("did not encounter a CRLF terminator")
		}

		// Error: found an isolated \r or \n, which is not allowed for simple strings or errors
		if (resp.Type == SimpleString || resp.Type == SimpleError) &&
			b[i] == '\n' && b[i-1] != '\r' || 
			b[i-1] == '\r' && b[i] != '\n' {
			return 0, nil, fmt.Errorf("simple strings/errors must not contain non-binary characters \\r or \\n")
		}
	}

	// If this isn't an array or bulk string and the message isn't being recursively parsed, 
	// and the crlf we found isn't at the very end of the string, then we have an error.
	if resp.Type != Array && resp.Type != BulkString && start == 0 && i != len(b)-1 {
		return 0, nil, fmt.Errorf("extra characters found after crlf terminator starting at index %d for non-array type", i)
	}

	// Handle Simple String (+string\r\n) and Simple Error (-Error message\r\n) types.
	if (resp.Type == SimpleError || resp.Type == SimpleString) {
		resp.Data = b[start+1 : i-1] 
		return i - start + 1, resp, nil // successfully parsed simple string/error
	}

	// Attempt to convert captured data to integer.
	strLength := string(b[start+1 : i-1])
	length, err := strconv.Atoi(strLength)

	// Error: Unable to parse as integer.
	if err != nil {
		return 0, nil, fmt.Errorf("unable to parse '%s' as an integer", strLength)
	}

	// Handle Integer type.
	// Integer format: :[<+|->]<value>\r\n
	if resp.Type == Integer {
		resp.Data = b[start+1 : i-1]  // it's stored as a string, not an actual integer
		return i - start + 1, resp, nil // successfully parsed integer
	}

	// Error: Type is Array and integer is negative.
	if length < 0 && resp.Type == Array {
		return 0, nil, fmt.Errorf("array length cannot be negative")
	}

	// Type is Bulk String and integer is -1 (null bulk string).
	if length == -1 && resp.Type == BulkString {
		resp.Data = nil
		return i - start + 1, resp, nil  // successfully parsed null bulk string
	} else if length < 0 && resp.Type == BulkString {
		return 0, nil, fmt.Errorf("bulk string length must not be negative, except for -1 for null bulk string")
	}

	i += 1
	// Handle Bulk String type
	// Bulk String format: $<length>\r\n<data>\r\n
	if resp.Type == BulkString {
		j := i

		// Error: Data length is too short
		if (i + length + 2) > len(b) {
			return 0, nil, fmt.Errorf("specified bulk string length does not match length of actual bulk string data")
		}

		i += (length + 1)

		// Error: Didn't find an \r\n after reading appropriate amount of data
		if !(b[i] == '\n' && b[i-1] == '\r') {
			return 0, nil, fmt.Errorf("did not find a crlf terminator after reading specified length of bulk string")
		}

		resp.Data = b[j : j+length]
		return i - start + 1, resp, nil  // successfully parsed bulk string
	}

	// Now for the absolute beast. Handle the Array type.
	// Array format: *<number-of-elements>\r\n<element-1>...<element-n>
	// Now "length" represents the number of elements in the array
	resp.ListRESPData = make([]*RESPData, length)
	for idx := 0; idx < length; idx++ {
		// Errror: Not enough array elements were able to be read.
		if i == len(b) {
			return 0, nil, fmt.Errorf("unable to read specified number of array elements")
		}

		// Recursively parse next element of array starting at current index i
		rread, rresp, rerr := decodeFromRESP(b, i)

		// Error: One of the array RESP elements was unsuccessfully parsed. Propagate error
		if rerr != nil {
			return 0, nil, rerr
		}

		// Add RESPData to list of array elements
		resp.ListRESPData[idx] = rresp
		// Update number of read bytes
		i += rread
	}

	return i, resp, nil  // successfully parsed array

}

func (r *RESPData) EncodeToRESP() (b []byte, success bool) {
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
			return RespNullString, true
		}

		strlen := len(r.Data)
		s := "$" + strconv.Itoa(strlen) + "\r\n" + string(r.Data) + "\r\n"
		return []byte(s), true
	case Array:
		if r.ListRESPData == nil {
			return RespNullArr, true
		}
		arrlen := len(r.ListRESPData)
		s := "*" + strconv.Itoa(arrlen) + "\r\n"
		for _, rr := range r.ListRESPData {
			res, suc := rr.EncodeToRESP()
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
