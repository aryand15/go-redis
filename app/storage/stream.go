package storage

import (
	"strings"
	"strconv"
	"github.com/aryand15/go-redis/resp"
	"fmt"
	"time"
)

// StreamEntry represents an entry in a Redis stream, and contains an id along with at least one key-value pair. 
// The id of a stream entry must be in the format "int-int", and the keys/values must be strings.
type StreamEntry struct { 
	id string 
	values map[string]string 
}

// Stream represents a Redis stream, containing multiple StreamEntry instances in order. 
// For any two stream entries, the ID of the one that comes after must be "bigger" than the other. 
// The CompareStreamIDs function can be used to determine the larger of two stream IDs.
type Stream struct {
	length int
	stream []*StreamEntry
}

// NewStream returns a fresh, unpopulated Stream object.
func NewStream() *Stream {
	return &Stream{
		length: 0,
		stream: make([]*StreamEntry, 0),
	}
}

// CompareStreamIDs compares two valid stream IDs.
// Returns -1 if idA < idB, 1 if idA > idB, and 0 if they are equal.
func CompareStreamIDs(idA string, idB string) int {
	idAParts := strings.Split(idA, "-")
	idBParts := strings.Split(idB, "-")

	millisA, _ := strconv.Atoi(idAParts[0])
	millisB, _ := strconv.Atoi(idBParts[0])

	if millisA < millisB {
		return -1
	} else if millisA > millisB {
		return 1
	} else {
		seqNumA, _ := strconv.Atoi(idAParts[1])
		seqNumB, _ := strconv.Atoi(idBParts[1])
		if seqNumA < seqNumB {
			return -1
		} else if seqNumA > seqNumB {
			return 1
		} else {
			return 0
		}
	}
}

// NewStreamEntry returns a fresh StreamEntry object with the given id.
func NewStreamEntry(id string) *StreamEntry {
	newEntry := &StreamEntry{
		id: id, 
		values: make(map[string]string)}
	return newEntry
}

// Length returns the number of stream entries in the Stream st.
func (st *Stream) Length() int {
	return st.length
}

// NextStreamID generates and returns the next valid stream ID for a future stream entry based on the most recent stream ID in st.
func (st *Stream) NextStreamID() string {
	if st.length == 0 {
		return fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0)
	}

	prevId := st.stream[st.length-1].id

	prevIdParts := strings.Split(prevId, "-")
	prevMillis, _ := strconv.Atoi(prevIdParts[0])
	prevSeqNum, _ := strconv.Atoi(prevIdParts[1])

	currMillis := time.Now().UnixMilli()
	seqNum := 0
	if currMillis < int64(prevMillis) {
		currMillis = int64(prevMillis)
	}

	if currMillis == int64(prevMillis) {
		seqNum = prevSeqNum + 1
	}

	nextId := fmt.Sprintf("%d-%d", currMillis, seqNum)

	return nextId
}

// AddEntry adds a new valid StreamEntry entry to Stream st.
func (st *Stream) AddEntry(entry *StreamEntry) {
	st.stream = append(st.stream, entry)
	st.length++
}

// EntryAt returns the stream entry at the given idx of Stream st.
func (st *Stream) EntryAt(idx int) *StreamEntry {
	return st.stream[idx]
}

// StreamData returns the stream data of Stream st as a list of StreamEntry instances.
func (st *Stream) StreamData() []*StreamEntry {
	return st.stream
}

// Slice returns a Stream containing all entries that are located at and after the start index. 
func (st *Stream) Slice(start int) ([]*StreamEntry, error) {
	if start < 0 || start >= st.Length() {
		return nil, fmt.Errorf("cannot take slice of stream with length %d starting at index %d", st.Length(), start)
	}
	sliceLen := st.Length()-start
	slicedStreamData := make([]*StreamEntry, sliceLen)
	copy(slicedStreamData, st.stream[start:])
	return slicedStreamData, nil
}

// CleanedNextStreamID takes a stream id, cleans it (replaces asterisks with real integers) and determines 
// if it can be used as the id for the next entry in the stream. If so, it returns the cleaned id; if not, 
// it returns an error.
func (st *Stream) CleanedNextStreamID(id string) (string, error) {
	// Cannot be 0-0
	if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	// Edge case: no previous entries and millis is 0
	} else if id == "0-*" && st.Length() == 0 {
		id = "0-1"
	// If ID is *, generate new ID based on previous entry (if exists)
	} else if id == "*" {
		id = st.NextStreamID()
	// Make sure ID is in the format int-int or int-*
	} else if idParts := strings.Split(id, "-"); len(idParts) != 2 {
		return "", fmt.Errorf("ERR invalid ID format")
	} else if millis, err1 := strconv.Atoi(idParts[0]); err1 != nil {
		return "", fmt.Errorf("ERR invalid ID format")
	} else if seqNum, err2 := strconv.Atoi(idParts[1]); err2 != nil && idParts[1] != "*" {
		return "", fmt.Errorf("ERR invalid ID format")
	// Make sure millis is greater than or equal to previous entry's millis
	} else if st.Length() > 0 && millis < st.stream[st.Length()-1].GetMillis() {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	// Handle int-* case
	} else if idParts[1] == "*" && st.Length() > 0 && millis == st.stream[st.Length()-1].GetMillis() {
		id = fmt.Sprintf("%d-%d", millis, st.stream[st.Length()-1].GetSeqNum()+1)
	} else if idParts[1] == "*" {
		id = fmt.Sprintf("%d-0", millis)
	// Make sure seqNum is greater than previous entry's seqNum if millis are equal
	} else if st.Length() > 0 && seqNum <= st.stream[st.Length()-1].GetSeqNum() && millis == st.stream[st.Length()-1].GetMillis() {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return id, nil
}


// Set assigns the given val to the key in the StreamEntry s.
func (s *StreamEntry) Set(key string, val string) {
	s.values[key] = val
}

// GetMillis returns the first part of the valid stream ID in StreamEntry s, where the valid stream ID is formatted as: "int-int"
func (s *StreamEntry) GetMillis() int {
	m, _ := strconv.Atoi(strings.Split(s.id, "-")[0])
	return m
}

// GetID returns the entire valid stream ID of StreamEntry s, where the valid stream ID is formatted as: "int-int".
func (s *StreamEntry) GetID() string {
	return s.id
}

// GetSeqNum returns the second part of the valid stream ID in StreamEntry s, where the valid stream ID is formatted as: "int-int"
func (s *StreamEntry) GetSeqNum() int {
	n, _ := strconv.Atoi(strings.Split(s.id, "-")[1])
	return n
}

// RESPData formats the data in StreamEntry s as a RESP array with the following format: 
//
// [id, [key1, val1, key2, val2] ]
//
// And returns the output as a RESPData structure.
func (s *StreamEntry) RESPData() *resp.RESPData {
	streamEntry := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 2)}

	// Add ID as first element of list
	respStreamId := &resp.RESPData{Type: resp.BulkString, Data: []byte(s.id)}
	streamEntry.ListRESPData[0] = respStreamId

	// Add list of keys & values as second element of list
	respKVList := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	for k := range s.values {
		respKVList.ListRESPData = append(respKVList.ListRESPData, resp.ConvertBulkStringToRESP(k), resp.ConvertBulkStringToRESP(s.values[k]))
	}
	streamEntry.ListRESPData[1] = respKVList

	return streamEntry
}