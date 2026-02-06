package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/aryand15/go-redis/app/client"
	"github.com/aryand15/go-redis/app/utils"
)

// ListInfo contains the list along with any clients blocked on BLPOP for the list.
type ListInfo struct {
	value        []string
	blpopWaiters *utils.Deque[*client.Client]
}

// StringInfo contains a string value along with an optional timer.
type StringInfo struct {
	value string
	timer *time.Timer
}

// StreamInfo contains the stream along with any clients blocked on XREAD for the stream.
type StreamInfo struct {
	value        *Stream
	xreadWaiters map[string](*utils.Set[*client.Client])
}

// DB represents the single datastore on which all clients perform operations. 
// It contains all supported data types, including strings, lists, and streams. 
// In addition, it contains fields to manage pub-sub 
// commands in an organized fashion, as well as a mutex to mitigate race conditions.
type DB struct {
	// Key-value pairs
	stringData map[string]*StringInfo

	// Lists
	listData map[string]*ListInfo

	// Streams
	streamData map[string]*StreamInfo

	// Mutex for concurrency
	mu sync.Mutex

	// Pub-sub
	subscribers map[string]*utils.Set[*client.Client] // publisher channel name -> set of receiving clients

}

// Lock locks the db.
func (db *DB) Lock() {
	db.mu.Lock()
}

// Unlock unlocks the db.
func (db *DB) Unlock() {
	db.mu.Unlock()
}


// GetSubscribers returns the set of clients who are currently subscribed to the
// channel named pub. It also returns a boolean indicating whether it was successful in finding the client list or not.
func (db *DB) GetSubscribers(pub string) (*utils.Set[*client.Client], bool) {
	val, ok := db.subscribers[pub]
	return val, ok
}

// AddSubscriber adds the given client to the subscriber list for this particular channel named pub.
// It creates the channel and a fresh subscriber list if it does not already exist in the DB.
// Returns an error if an update cannot be made to the client state.
func (db *DB) AddSubscriber(c *client.Client, pub string) error {
	if _, ok := db.GetSubscribers(pub); !ok {
		db.subscribers[pub] = utils.NewSet[*client.Client]()
	}
	db.subscribers[pub].Add(c)
	return c.AddPublisher(pub)
}

// RemoveSubscriber removes the given client from the subscriber list for this particular channel named pub,
// removing the channel from existence if there are no subscribed clients left.
// If the channel doesn't exist to begin with, it is a no-op.
// Returns an error if an update cannot be made to the client state.
func (db *DB) RemoveSubscriber(c *client.Client, pub string) error {
	if set, ok := db.GetSubscribers(pub); ok {
		set.Remove(c)
		if set.Length() == 0 {
			delete(db.subscribers, pub)
		}
		return c.RemovePublisher(pub)
	}
	return nil
}

// SetString assigns the given string key to the given val. If the key doesn't exist yet,
// and a non-string type (stream, list, etc.) uses this key, then it is successful. If the key had been previously
// been set with an expiration, and still exists, then the timer is cleared. It returns an error if the key already exists in a non-string type.
func (db *DB) SetString(key string, val string) error {
	// Make sure no other data type has the same key
	if !db.CanSetString(key) {
		return fmt.Errorf("key '%s' with non-string data type already exists", key)
	}

	// If a timer already exists, remove it
	if sData, ok := db.stringData[key]; ok && sData.timer != nil {
		sData.timer.Stop()
	} else if !ok {
		db.stringData[key] = &StringInfo{}
	}
	db.stringData[key].value = val
	return nil
}

// TimedSetString assigns the given string key to the given val, automatically setting a timer with which the key is deleted
// after the specified duration. If a timer already exists, it is overridden with this duration. Other than that, the behavior is similar
// to SetString.
func (db *DB) TimedSetString(key string, val string, duration time.Duration) error {
	// Make sure no other data type has the same key
	if !db.CanSetString(key) {
		return fmt.Errorf("key '%s' with non-string data type already exists", key)
	}

	// If a timer already exists, stop it so that key doesn't get deleted twice
	if sData, ok := db.stringData[key]; ok && sData.timer != nil {
		sData.timer.Stop()
	} else if !ok {
		db.stringData[key] = &StringInfo{}
	}
	db.stringData[key].value = val
	db.stringData[key].timer = time.AfterFunc(duration, func() {
		db.removeString(key)
	})

	return nil
}

// GetString returns the string value associated with the given key, if it exists.
// It also returns a boolean indicating whether the string key exists or not.
func (db *DB) GetString(key string) (string, bool) {
	sData, ok := db.stringData[key]
	if !ok {
		return "", false
	}
	return sData.value, true
}

// removeString deletes the string key from the database, and is a no-op if the key doesn't exist.
// A mutex is used here, as can be executed asynchronously when the timer for the key expires.
func (db *DB) removeString(key string) {
	// Use mutex as this code can be executed at any point
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.stringData, key)
}

// GetList returns the list value associated with the given key, if it exists.
// It also returns a boolean indicating whether the list key exists or not.
func (db *DB) GetList(key string) ([]string, bool) {
	lData, ok := db.listData[key]
	if !ok || lData.value == nil {
		return nil, false
	}
	return lData.value, true
}

// GetListRange returns the elements of the given list value that fall between the start and stop indices, inclusive. 
// If the key doesn't exist yet, or the range is out of bounds, then it is returns an error.
func (db *DB) GetListRange(key string, start int, stop int) ([]string, error) {
	list, ok := db.GetList(key)
	if !ok {
		return nil, fmt.Errorf("key '%s' doesn't exist", key)
	}
	if start < 0 || stop >= len(list) {
		return nil, fmt.Errorf("Invalid range: [%d, %d] for list of length %d", start, stop, len(list))
	}

	ret := make([]string, stop-start+1)
	copy(ret, list[start:stop+1])
	return ret, nil
}

// SetList assigns the given list key to the given list val. If the key doesn't exist yet, and a non-list type
// (string, stream, etc.) uses this key, then it is returns an error.
func (db *DB) SetList(key string, val []string) error {
	// Make sure no other data type has the same key
	if !db.CanSetList(key) {
		return fmt.Errorf("key '%s' with non-list data type already exists", key)
	}

	if _, ok := db.listData[key]; !ok {
		db.listData[key] = &ListInfo{}
	}
	db.listData[key].value = val
	return nil
}

// AppendToList adds the given elem key to the tail of the list with the given key.
// If the key doesn't exist yet, then it is returns an error.
func (db *DB) AppendToList(key string, elem string) error {
	if _, ok := db.GetList(key); !ok {
		return fmt.Errorf("key '%s' doesn't exist", key)
	}

	if db.listData[key].value == nil {
		db.listData[key].value = make([]string, 0)
	}

	db.listData[key].value = append(db.listData[key].value, elem)
	return nil
}

// PrependToList adds the given elem key to the head of the list with the given key.
// If the key doesn't exist yet, then it returns an error.
func (db *DB) PrependToList(key string, elem string) error {
	if _, ok := db.GetList(key); !ok {
		return fmt.Errorf("key '%s' doesn't exist", key)
	}

	if db.listData[key].value == nil {
		db.listData[key].value = make([]string, 0)
	}

	db.listData[key].value = append([]string{elem}, db.listData[key].value...)
	return nil
}

// PopLeftList deletes and returns the first element from the head of the list with the given key.
// If the key doesn't exist yet, or the list is empty, then it returns an error.
func (db *DB) PopLeftList(key string) (string, error) {
	if list, ok := db.GetList(key); !ok || len(list) == 0 {
		return "", fmt.Errorf("key '%s' doesn't exist or is empty", key)
	}

	popped := db.listData[key].value[0]
	db.listData[key].value = db.listData[key].value[1:]
	return popped, nil
}

// PopRightList deletes and returns the last element from the tail of the list with the given key.
// If the key doesn't exist yet, or the list is empty, then it returns an error.
func (db *DB) PopRightList(key string) (string, error) {
	if list, ok := db.GetList(key); !ok || len(list) == 0 {
		return "", fmt.Errorf("key '%s' doesn't exist or is empty", key)
	}

	lastIndex := len(db.listData[key].value)-1
	popped := db.listData[key].value[lastIndex]
	db.listData[key].value = db.listData[key].value[:lastIndex]
	return popped, nil
}

// GetXREADIDWaiters returns a map, where the key is a stream ID, and the value is the set of all client channels blocked on XREAD,
// waiting for an ID that is greater than that stream ID to be added to the stream with the given name.
// It also returns a boolean indicating whether it was able to find any clients that are blocked on XREAD for this stream.
func (db *DB) GetXREADIDWaiters(sname string) (map[string]*utils.Set[*client.Client], bool) {
	sInfo, ok := db.streamData[sname]
	if !ok || sInfo.xreadWaiters == nil {
		return nil, false
	}
	return sInfo.xreadWaiters, ok
}

// AddXREADIDWaiter takes a stream name, a stream ID, and registers the client receiver channel to receive an update from the
// blocking XREAD call when an entry is added to the specified stream whose ID is greater than the given id.
// Returns error if client state unable to be updated.
func (db *DB) AddXREADIDWaiter(sname string, id string, c *client.Client) error {
	if _, ok := db.streamData[sname]; !ok {
		db.streamData[sname] = &StreamInfo{xreadWaiters: make(map[string]*utils.Set[*client.Client])}
	} else if ok && db.streamData[sname].xreadWaiters == nil {
		db.streamData[sname].xreadWaiters = make(map[string]*utils.Set[*client.Client])
	}
	
	if _, ok := db.streamData[sname].xreadWaiters[id]; !ok {
		db.streamData[sname].xreadWaiters[id] = utils.NewSet[*client.Client]()
	}
	db.streamData[sname].xreadWaiters[id].Add(c)
	return c.AddXREADId(sname, id)
}

// RemoveXREADIDWaiter takes a stream name, a stream ID, and a client receiver channel. It removes the client from the list of clients
// that are blocked on XREAD waiting for an entry to be added to the specified stream whose ID is greater than the given id.
// It returns an error if the stream doesn't exist.
func (db *DB) RemoveXREADIDWaiter(sname string, id string, c *client.Client) error {
	sInfo, ok := db.streamData[sname]
	if !ok || sInfo.xreadWaiters == nil {
		return fmt.Errorf("stream with key %s does not have any xread waiters", sname)
	}
	idWaiters, ok := sInfo.xreadWaiters[id]
	if !ok || !idWaiters.Has(c) {
		return fmt.Errorf("unable to find the client blocked on XREAD for stream IDs greater than %s for stream with key %s", id, sname)
	}

	idWaiters.Remove(c)
	if idWaiters.Length() == 0 {
		delete(sInfo.xreadWaiters, id)
	}

	return c.RemoveXREADID(sname, id)
}

// GetXREADAllWaiters returns a set of all client channels blocked on XREAD, waiting for any new entry to be
// added to the stream with the given name.  It also returns a boolean indicating whether it was able to find the set or not. 
// It returns an error if the stream doesn't exist.
func (db *DB) GetXREADAllWaiters(sname string) (*utils.Set[*client.Client], error) {
	sInfo, ok := db.streamData[sname]
	if !ok {
		return nil, fmt.Errorf("stream with key %s does not exist", sname)
	}
	xReadAllWaiters, ok := sInfo.xreadWaiters["0-0"]
	if !ok {
		return utils.NewSet[*client.Client](), nil
	}

	return xReadAllWaiters, nil
}

// AddXREADAllWaiter takes a stream name, and registers the client receiver channel to receive an update from the
// blocking XREAD call when any entry is added to the specified stream. It returns an error if the stream doesn't exist.
func (db *DB) AddXREADAllWaiter(sname string, c *client.Client) error {
	return db.AddXREADIDWaiter(sname, "0-0", c)
}

// RemoveXREADAllWaiter takes a stream name and a client receiver channel. It removes the client from the list of clients
// that are blocked on XREAD waiting for any entry to be added to the specified stream.
// It returns an error if the stream doesn't exist or if the client receiver channel was not found.
func (db *DB) RemoveXREADAllWaiter(sname string, c *client.Client) error {
	return db.RemoveXREADIDWaiter(sname, "0-0", c)
}

// GetNextBLPOPWaiter finds the client next in queue that is blocked on the BLPOP command and that is waiting for an element
// to be pushed to the list with the specified key, and returns its receiver channel. It also returns a boolean indicating whether or not
// any clients are blocked on this key.
func (db *DB) GetNextBLPOPWaiter(key string) (*client.Client, bool) {
	lInfo, ok := db.listData[key]
	if !ok {
		return nil, false
	}
	waiters := lInfo.blpopWaiters
	if waiters == nil || waiters.IsEmpty() {
		return nil, false
	}
	ret, _ := waiters.PeekLeft()
	return ret, true
}

// AddBLPOPWaiter pushes a client onto the queue of clients that are blocked on the BLPOP command and that are waiting for an element
// to be pushed to the list with the specified key. It's a no-op if the client is already blocked on that key, and it returns 
// an error if client is in subscribe mode or conducting a transaction.
func (db *DB) AddBLPOPWaiter(key string, c *client.Client) error {
	if _, ok := db.listData[key]; !ok {
		db.listData[key] = &ListInfo{
			blpopWaiters: utils.NewDeque[*client.Client](),
		}
	}
	if keys, _ := c.GetBlpopKeys(); !keys.Has(key) {
		db.listData[key].blpopWaiters.PushRight(c)
		return c.AddBlpopKey(key)
	}
	return nil
}

// PopBLPOPWaiter is similar GetNextBLPOPWaiter in that it finds the client next in queue that is blocked on the BLPOP command,
// but it takes the additional step of removing it from the queue. It returns an error if the queue is empty or if the client 
// state was unable to be updated.
func (db *DB) PopBLPOPWaiter(key string, c *client.Client) error {
	lInfo, ok := db.listData[key]
	if !ok {
		return fmt.Errorf("list with key %s does not exist", key)
	}
	waiters := lInfo.blpopWaiters
	_, err := waiters.PopLeft()
	if err != nil {
		return err
	}

	return c.RemoveBlpopKey(key)
}

// RemoveBLPOPWaiter is almost the same as PopBLPOPWaiter, but instead of removing the first element in the queue of clients
// blocked on BLPOP for this list key, it removes a specified client from the queue. It returns an error
// if the list with the specified key does not exist, and it's a no-op if it was unable to find the specified receiver channel in the queue.
func (db *DB) RemoveBLPOPWaiter(key string, c *client.Client) error {
	lInfo, ok := db.listData[key]
	if !ok || lInfo.blpopWaiters == nil {
		return fmt.Errorf("list with key %s does not exist", key)
	}

	waiters := lInfo.blpopWaiters
	if err := waiters.DeleteVal(c); err != nil {
		return err
	}
	return c.RemoveBlpopKey(key)
}

// GetStream returns the stream under the specified key as a Stream object.
// It also returns a boolean indicating whether the stream exists under the specified key or not.
func (db *DB) GetStream(key string) (*Stream, bool) {
	sInfo, ok := db.streamData[key]
	if !ok || sInfo.value == nil {
		return nil, false
	}
	return sInfo.value, true
}

// SetStream sets the stream under the specified stream key to the given value, creating the stream if it doesn't exist already.
// It returns an error if any other non-stream data type (string, list, etc.) already uses that key.
func (db *DB) SetStream(key string, val *Stream) error {
	// Make sure no other data type has the same key
	if !db.CanSetStream(key) {
		return fmt.Errorf("key '%s' is taken already", key)
	}
	if _, ok := db.streamData[key]; !ok {
		db.streamData[key] = &StreamInfo{}
	}

	db.streamData[key].value = val
	return nil
}

// CanSetList returns true if the given key is available to use for list data, and false if it is already a string
// and/or stream key.
func (db *DB) CanSetList(key string) bool {
	_, stringOk := db.stringData[key]
	_, streamOk := db.streamData[key]
	return !(stringOk || streamOk)
}

// CanSetString returns true if the given key is available to use for string data, and false if it is already a list
// and/or stream key.
func (db *DB) CanSetString(key string) bool {
	_, listOk := db.listData[key]
	_, streamOk := db.streamData[key]
	return !(listOk || streamOk)
}

// CanSetStream returns true if the given key is available to use for stream data, and false if it is already a list
// and/or string key.
func (db *DB) CanSetStream(key string) bool {
	_, listOk := db.listData[key]
	_, stringOk := db.stringData[key]
	return !(listOk || stringOk)
}

// NewDB initializes a new DB object with empty values for each of its fields.
func NewDB() *DB {
	return &DB{
		stringData:   make(map[string]*StringInfo),
		listData:     make(map[string]*ListInfo),
		streamData:   make(map[string]*StreamInfo),
		subscribers:  make(map[string]*utils.Set[*client.Client]),
	}
}
