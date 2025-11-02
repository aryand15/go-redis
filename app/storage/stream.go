package storage

import (
	"strings"
	"strconv"
	"github.com/aryand15/go-redis/resp"
)

type StreamEntry struct { 
	id string 
	values map[string]string 
}

func NewStreamEntry(id string) *StreamEntry {
	newEntry := &StreamEntry{
		id: id, 
		values: make(map[string]string)}
	return newEntry
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