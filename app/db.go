package main

import (
	"sync"
	"time"
	"fmt"
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

	// Mutex for concurrency
	mu sync.Mutex 
}

type StreamEntry struct { 
	id string 
	values map[string]string 
}

func (s *StreamEntry) GetMillis() int {
	var millis int
	fmt.Sscanf(s.id, "%d-", &millis)
	return int(millis)
}

func (s *StreamEntry) GetSeqNum() int {
	var seqNum int
	fmt.Sscanf(s.id, "-%d", &seqNum)
	return seqNum
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
	}
}