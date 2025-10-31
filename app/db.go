package main

import (
	"sync"
	"time"
	"strings"
	"strconv"
	"net"
)


type DB struct {
	// Key-value pairs 
	stringData map[string]string 
	stringTimers map[string]*time.Timer

	// Lists
	listData map[string]([]string) 
	blpopWaiters map[string]([]chan string)

	// Streams
	streamData map[string]([]*StreamEntry)
	// Format: stream name -> ID -> list of channels waiting for new entries after ID
	xreadIdWaiters map[string](map[string]([]chan *StreamEntry))
	// Format: stream name -> list of channels waiting for any new entries
	xreadAllWaiters map[string]([]chan *StreamEntry)

	// Mutex for concurrency
	mu sync.Mutex

	// Map of connections that are in the process of building transactions
	transactions map[net.Conn]([][]byte)

	// Pub-sub
	subscribers map[string]*Set[chan string] // Maps publisher channel name to set of receiving clients
	publishers map[net.Conn]*Set[string] // Maps client to set of subscribed publisher channels

}

type StreamEntry struct { 
	id string 
	values map[string]string 
}

type Set[T comparable] struct {
	set map[T]struct{}
	length int
}

func (s *Set[T]) Add(entry T) {
	if _, ok := s.set[entry]; ok {
		s.set[entry] = struct{}{}
		s.length++
	}
}

func (s *Set[T]) Remove(entry T) {
	if _, ok := s.set[entry]; ok {
		delete(s.set, entry)
		s.length--
	}
}

func (s *StreamEntry) GetMillis() int {
	m, _ := strconv.Atoi(strings.Split(s.id, "-")[0])
	return m
}

func (s *StreamEntry) GetSeqNum() int {
	n, _ := strconv.Atoi(strings.Split(s.id, "-")[1])
	return n
}

func (s *StreamEntry) RESPData() *RESPData {
	streamEntry := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 2)}

	// Add ID as first element of list
	respStreamId := &RESPData{Type: BulkString, Data: []byte(s.id)}
	streamEntry.ListRESPData[0] = respStreamId

	// Add list of keys & values as second element of list
	respKVList := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 0)}
	for k := range s.values {
		respKVList.ListRESPData = append(respKVList.ListRESPData, ConvertBulkStringToRESP(k))
		respKVList.ListRESPData = append(respKVList.ListRESPData, ConvertBulkStringToRESP(s.values[k]))
	}
	streamEntry.ListRESPData[1] = respKVList

	return streamEntry
}


func (db *DB) SetString(key string, val string) {
	// Make sure no other data type has the same key
	if !db.CanSetString(key) {
		return
	}

	// If a timer already exists, remove it
	if timer, ok := db.stringTimers[key]; ok {
		timer.Stop()
	}
	db.stringData[key] = val
	

}



func (db *DB) TimedSetString(key string, val string, duration time.Duration) {
	// Make sure no other data type has the same key
	if !db.CanSetString(key) {
		return
	}

	// If a timer already exists, stop it so that key doesn't get deleted twice
	if timer, ok := db.stringTimers[key]; ok{
		timer.Stop()
	}

	db.stringData[key] = val
	db.stringTimers[key] = time.AfterFunc(duration, func() {db.removeString(key)})
	
}

func (db *DB) GetString(key string) (string, bool) {
	val, ok := db.stringData[key]
	return val, ok
}

func (db *DB) removeString(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.stringData, key)
	delete(db.stringTimers, key)
}

func (db *DB) GetList(key string) ([]string, bool) {
	val, ok := db.listData[key]
	return val, ok
}

func (db *DB) SetList(key string, val []string) {
	// Make sure no other data type has the same key
	if !db.CanSetList(key) {
		return
	}

	db.listData[key] = val
}

func (db *DB) GetStream(key string) ([]*StreamEntry, bool) {
	val, ok := db.streamData[key]
	return val, ok
}

func (db *DB) SetStream(key string, val []*StreamEntry) {
	// Make sure no other data type has the same key
	if !db.CanSetStream(key) {
		return
	}

	db.streamData[key] = val
}


func (db *DB) CanSetList(key string) bool {
	_, stringOk := db.stringData[key];
	_, streamOk := db.streamData[key];
	return !(stringOk || streamOk)
}

func (db *DB) CanSetString(key string) bool {
	_, listOk := db.listData[key];
	_, streamOk := db.streamData[key];
	return !(listOk || streamOk)
}

func (db *DB) CanSetStream(key string) bool {
	_, listOk := db.listData[key];
	_, stringOk := db.stringData[key];
	return !(listOk || stringOk)
}


func NewDB() *DB {
	return &DB{
		stringData:  make(map[string]string),
		stringTimers: make(map[string]*time.Timer),
		listData: make(map[string]([]string)),
		blpopWaiters: make(map[string]([]chan string)),
		streamData: make(map[string]([]*StreamEntry)),
		xreadIdWaiters: make(map[string](map[string]([]chan *StreamEntry))),
		xreadAllWaiters: make(map[string]([]chan *StreamEntry)),
		transactions: make(map[net.Conn]([][]byte)),
		subscribers: make(map[string]*Set[chan string]),
		publishers: make(map[net.Conn]*Set[string]),
	}
}