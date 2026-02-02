package storage

import (
	"strings"
	"strconv"
	"github.com/aryand15/go-redis/resp"
	"fmt"
	"time"
)

type StreamEntry struct { 
	id string 
	values map[string]string 
}

type Stream struct {
	length int
	stream []*StreamEntry
}

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

func NewStreamEntry(id string) *StreamEntry {
	newEntry := &StreamEntry{
		id: id, 
		values: make(map[string]string)}
	return newEntry
}

func (st *Stream) Length() int {
	return st.length
}

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

	return fmt.Sprintf("%d-%d", currMillis, seqNum)
}

func (st *Stream) AddEntry(entry *StreamEntry) {
	st.stream = append(st.stream, entry)
	st.length++
}

func (st *Stream) EntryAt(idx int) *StreamEntry {
	return st.stream[idx]
}

func (st *Stream) StreamData() []*StreamEntry {
	return st.stream
}

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



func (s *StreamEntry) Set(key string, val string) {
	s.values[key] = val
}

func (s *StreamEntry) GetMillis() int {
	m, _ := strconv.Atoi(strings.Split(s.id, "-")[0])
	return m
}

func (s *StreamEntry) GetID() string {
	return s.id
}

func (s *StreamEntry) GetSeqNum() int {
	n, _ := strconv.Atoi(strings.Split(s.id, "-")[1])
	return n
}

func (s *StreamEntry) RESPData() *resp.RESPData {
	streamEntry := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 2)}

	// Add ID as first element of list
	respStreamId := &resp.RESPData{Type: resp.BulkString, Data: []byte(s.id)}
	streamEntry.ListRESPData[0] = respStreamId

	// Add list of keys & values as second element of list
	respKVList := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	for k := range s.values {
		respKVList.ListRESPData = append(respKVList.ListRESPData, resp.ConvertBulkStringToRESP(k))
		respKVList.ListRESPData = append(respKVList.ListRESPData, resp.ConvertBulkStringToRESP(s.values[k]))
	}
	streamEntry.ListRESPData[1] = respKVList

	return streamEntry
}