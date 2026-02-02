package storage

import (
	"net"
	"sync"
	"time"
	"github.com/aryand15/go-redis/resp"
)

type DB struct {
	// Key-value pairs
	stringData   map[string]string // key -> value
	stringTimers map[string]*time.Timer // key -> timer

	// Lists
	listData     map[string]([]string) // key -> array
	blpopWaiters map[string]([]chan string) // key -> queue of clients waiting on a BLPOP operation

	// Streams
	streamData map[string](*Stream) // key -> [streamentry1, streamentry2, ...]
	xreadIdWaiters map[string](map[string]([]chan *StreamEntry)) // stream name -> ID -> queue of channels waiting for new entries after ID
	xreadAllWaiters map[string]([]chan *StreamEntry) // stream name -> queue of channels waiting for any new entries

	// Mutex for concurrency
	mu sync.Mutex

	// Transactions
	transactions map[net.Conn]([]*resp.RESPData) // client -> [msg1, msg2, ...]


	// Pub-sub
	subscribers map[string]*Set[net.Conn] // publisher channel name -> set of receiving clients
	publishers  map[net.Conn]*Set[string] // client -> set of subscribed publisher channels
	receivers   map[net.Conn]chan string  // client -> channel for receiving updates

}

func (db *DB) Lock() {
	db.mu.Lock()
}

func (db *DB) Unlock() {
	db.mu.Unlock()
}

func (db *DB) GetTransaction(client net.Conn) ([]*resp.RESPData, bool) {
	val, ok := db.transactions[client]
	return val, ok
}

func (db *DB) DeleteTransaction(client net.Conn) {
	delete(db.transactions, client)
}

func (db *DB) CreateTransaction(client net.Conn) {
	db.transactions[client] = make([]*resp.RESPData, 0)
}

func (db *DB) AddToTransaction(client net.Conn, request *resp.RESPData) {
	if _, ok := db.GetTransaction(client); !ok {
		db.CreateTransaction(client)
	}
	db.transactions[client] = append(db.transactions[client], request)

}

func (db *DB) GetSubscribers(pub string) (*Set[net.Conn], bool) {
	val, ok := db.subscribers[pub]
	return val, ok
}

func (db *DB) AddSubscriber(pub string, client net.Conn) {
	if _, ok := db.GetSubscribers(pub); !ok {
		db.subscribers[pub] = NewSet[net.Conn]()
	}
	db.subscribers[pub].Add(client)
}

func (db *DB) RemoveSubscriberFromPublisher(pub string, client net.Conn) {
	if set, ok := db.GetSubscribers(pub); ok {
		set.Remove(client)
		if set.length == 0 {
			delete(db.subscribers, pub)
		}
	}
}

func (db *DB) CreateReceiver(client net.Conn) {
	if _, ok := db.GetReceiver(client); !ok {
		ch := make(chan string)
		db.receivers[client] = ch
	}
}

func (db *DB) GetReceiver(client net.Conn) (chan string, bool) {
	ch, ok := db.receivers[client]
	return ch, ok
}

func (db *DB) GetPublishers(client net.Conn) (*Set[string], bool) {
	val, ok := db.publishers[client]
	return val, ok
}

func (db *DB) AddPublisher(client net.Conn, pub string) {
	if _, ok := db.GetPublishers(client); !ok {
		db.publishers[client] = NewSet[string]()
	}
	db.publishers[client].Add(pub)
}

func (db *DB) RemovePublisherFromSubscriber(client net.Conn, pub string) {
	if set, ok := db.GetPublishers(client); ok {
		set.Remove(pub)
		if set.length == 0 {
			delete(db.publishers, client)
		}
	}
}

func(db *DB) DeleteSubscriber(client net.Conn) {
	delete(db.publishers, client)  // Client no longer has any subscriptions
	delete(db.receivers, client)  // Client no longer needs a receive channel
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
	if timer, ok := db.stringTimers[key]; ok {
		timer.Stop()
	}

	db.stringData[key] = val
	db.stringTimers[key] = time.AfterFunc(duration, func() {
		db.removeString(key)
	})

}

func (db *DB) GetString(key string) (string, bool) {
	val, ok := db.stringData[key]
	return val, ok
}

func (db *DB) removeString(key string) {
	// Use mutex as this code can be executed at any point
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.stringData, key)
	delete(db.stringTimers, key)
}

func (db *DB) GetList(key string) ([]string, bool) {
	val, ok := db.listData[key]
	return val, ok
}

// SetList sets a list with specified name (key) to val.
// Does nothing if key is already taken.
func (db *DB) SetList(key string, val []string) {
	// Make sure no other data type has the same key
	if !db.CanSetList(key) {
		return
	}

	db.listData[key] = val
}

func (db *DB) AppendToList(key string, elem string) {
	if _, ok := db.GetList(key); !ok {
		return
	}

	db.listData[key] = append(db.listData[key], elem)
}

func (db *DB) PrependToList(key string, elem string) {
	if _, ok := db.GetList(key); !ok {
		return
	}

	db.listData[key] = append([]string{elem}, db.listData[key]...)
}

func (db *DB) PopLeftList(key string) {
	if list, ok := db.GetList(key); !ok || len(list) == 0 {
		return
	}

	db.listData[key] = db.listData[key][1:]

}

func (db *DB) GetXREADIDWaiters(sname string) (map[string][]chan *StreamEntry, bool) {
	waiters, ok := db.xreadIdWaiters[sname]
	return waiters, ok
}

func (db *DB) AddXREADIDWaiter(sname string, id string, receiver chan *StreamEntry) {
	_, ok := db.xreadIdWaiters[sname]
	if !ok {
		db.xreadIdWaiters[sname] = make(map[string]([]chan *StreamEntry))
	}
	_, ok = db.xreadIdWaiters[sname][id]
	if !ok {
		db.xreadIdWaiters[sname][id] = make([]chan *StreamEntry, 0)
	}
	db.xreadIdWaiters[sname][id] = append(db.xreadIdWaiters[sname][id], receiver)
}

func (db *DB) RemoveXREADIDWaiter(sname string, id string, idx int) {
	allWaiters, streamOk := db.xreadIdWaiters[sname]
	if !streamOk {
		db.xreadIdWaiters[sname] = make(map[string]([]chan *StreamEntry))
	}
	waiters, ok := allWaiters[id]
	if !ok || idx < 0 || idx >= len(waiters) {
		return
	}
	db.xreadIdWaiters[sname][id] = append(waiters[:idx], waiters[idx+1:]...)
}

func (db *DB) GetXREADAllWaiters(sname string) ([]chan *StreamEntry, bool) {
	waiters, ok := db.xreadAllWaiters[sname]
	return waiters, ok
}

func (db *DB) AddXREADAllWaiter(sname string, receiver chan *StreamEntry) {
	_, ok := db.xreadAllWaiters[sname]
	if !ok {
		db.xreadAllWaiters[sname] = make([]chan *StreamEntry, 0)
	}

	db.xreadAllWaiters[sname] = append(db.xreadAllWaiters[sname], receiver)
}

func (db *DB) RemoveXREADAllWaiter(key string, idx int) {
	waiters, ok := db.xreadAllWaiters[key]
	if !ok || idx < 0 || idx >= len(waiters) {
		return
	}
	db.xreadAllWaiters[key] = append(waiters[:idx], waiters[idx+1:]...)
}

func (db *DB) GetBLPOPWaiters(key string) ([]chan string, bool) {
	waiters, ok := db.blpopWaiters[key]
	return waiters, ok
}

func (db *DB) PopBLPOPWaiter(key string) {
	if waiters, ok := db.blpopWaiters[key]; !ok || len(waiters) == 0 {
		return
	}

	db.blpopWaiters[key] = db.blpopWaiters[key][1:]
}

func (db *DB) AddBLPOPWaiter(key string) chan string {
	if _, ok := db.GetBLPOPWaiters(key); !ok {
		db.blpopWaiters[key] = make([]chan string, 0)
	}
	c := make(chan string)
	db.blpopWaiters[key] = append(db.blpopWaiters[key], c)
	return c
}

func (db *DB) RemoveBLPOPWaiter(key string, idx int) {
	waiters, ok := db.blpopWaiters[key]
	if !ok || idx < 0 || idx >= len(waiters) {
		return
	}

	if len(waiters) == 1 {
		delete(db.blpopWaiters, key)
	} else {
		db.blpopWaiters[key] = append(waiters[:idx], waiters[idx+1:]...)
	}
}

func (db *DB) GetStream(key string) (*Stream, bool) {
	val, ok := db.streamData[key]
	return val, ok
}

func (db *DB) SetStream(key string, val *Stream) {
	// Make sure no other data type has the same key
	if !db.CanSetStream(key) {
		return
	}

	db.streamData[key] = val
}

func (db *DB) CanSetList(key string) bool {
	_, stringOk := db.stringData[key]
	_, streamOk := db.streamData[key]
	return !(stringOk || streamOk)
}

func (db *DB) CanSetString(key string) bool {
	_, listOk := db.listData[key]
	_, streamOk := db.streamData[key]
	return !(listOk || streamOk)
}

func (db *DB) CanSetStream(key string) bool {
	_, listOk := db.listData[key]
	_, stringOk := db.stringData[key]
	return !(listOk || stringOk)
}

func NewDB() *DB {
	return &DB{
		stringData:      make(map[string]string),
		stringTimers:    make(map[string]*time.Timer),
		listData:        make(map[string]([]string)),
		blpopWaiters:    make(map[string]([]chan string)),
		streamData:      make(map[string](*Stream)),
		xreadIdWaiters:  make(map[string](map[string]([]chan *StreamEntry))),
		xreadAllWaiters: make(map[string]([]chan *StreamEntry)),
		transactions:    make(map[net.Conn]([]*resp.RESPData)),
		subscribers:     make(map[string]*Set[net.Conn]),
		publishers:      make(map[net.Conn]*Set[string]),
		receivers:       make(map[net.Conn]chan string),
	}
}
