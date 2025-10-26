package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Common RESP responses
var (
	respOK         = []byte("+OK\r\n")
	respPong       = []byte("+PONG\r\n")
	respNullString = []byte("$-1\r\n")
	respEmptyArr   = []byte("*0\r\n")
	respNullArr  = []byte("*-1\r\n")
	respNoneString = []byte("+none\r\n")
)

type CommandHandler struct {
	db *DB
}

func NewCommandHandler(db *DB) *CommandHandler {
	return &CommandHandler{db: db}
}

func (h *CommandHandler) HandleEcho(args []*RESPData) ([]byte, bool) {
	if len(args) < 2 {
		return nil, false
	}
	return EncodeToRESP(&RESPData{Type: BulkString, Data: args[1].Data})
}

func (h *CommandHandler) HandlePing() ([]byte, bool) {
	return respPong, true
}

func (h *CommandHandler) HandleSet(args []*RESPData) ([]byte, bool) {
	if len(args) != 3 && len(args) != 5 {
		return nil, false
	}
	key := string(args[1].Data)
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	if len(args) == 3 {
		h.db.Set(key, args[2])
	} else if timeOption := string(args[3].Data); timeOption != "PX" && timeOption != "EX" {
		return nil, false
	} else if duration, err := strconv.Atoi(string(args[4].Data)); err != nil || duration < 0 {
		return nil, false
	} else if timeOption == "EX" {
		h.db.TimedSet(key, args[2], time.Duration(duration)*time.Second)
	} else if timeOption == "PX" {
		h.db.TimedSet(key, args[2], time.Duration(duration)*time.Millisecond)
	}
	return respOK, true
}

func (h *CommandHandler) HandleGet(args []*RESPData) ([]byte, bool) {
	if len(args) < 2 {
		return nil, false
	}
	key := string(args[1].Data)
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	val, ok := h.db.Get(key)
	if !ok {
		return respNullString, true
	}
	return EncodeToRESP(val)
}

func (h *CommandHandler) HandleRPush(args []*RESPData) ([]byte, bool) {
	if len(args) < 3 {
		return nil, false
	}

	// The name of the array for which we want to push
	key := string(args[1].Data)

	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	val, ok := h.db.Get(key)

	// If array doesn't exist, create an empty array belonging to that key
	if !ok || val.Type != Array {
		val = &RESPData{Type: Array, NestedRESPData: make([]*RESPData, 0)}
		h.db.Set(key, val)
	}

	// Add all elements to the array
	for i := 2; i < len(args); i++ {
		val.NestedRESPData = append(val.NestedRESPData, CloneRESP(args[i]))
	}

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if waitChans, ok := h.db.waiters[key]; ok {
		popped := val.NestedRESPData[0]
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0])    // Close the channel

		h.db.waiters[key] = h.db.waiters[key][1:] // Remove client off the queue
	}
	// Return length of array
	newLen := strconv.Itoa(len(val.NestedRESPData))
	return EncodeToRESP(
		&RESPData{
			Type: Integer,
			Data: []byte(newLen),
		})

}

func (h *CommandHandler) HandleLPush(args []*RESPData) ([]byte, bool) {
	if len(args) < 3 {
		return nil, false
	}

	key := string(args[1].Data)

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	val, ok := h.db.Get(key)

	// If the key doesn't exist as an array, create a new empty array
	if !ok || val.Type != Array {
		val = &RESPData{Type: Array, NestedRESPData: make([]*RESPData, 0)}
		h.db.Set(key, val)
	}

	// Create a new bigger array to hold the old + newly appended elements
	newArr := make([]*RESPData, len(val.NestedRESPData)+len(args)-2)
	for i := 0; i < len(args)-2; i++ {
		newArr[i] = CloneRESP(args[len(args)-1-i])
	}
	for i := len(args) - 2; i < len(newArr); i++ {
		newArr[i] = CloneRESP(val.NestedRESPData[i-len(args)+2])
	}
	val.NestedRESPData = newArr

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if waitChans, ok := h.db.waiters[key]; ok {
		popped := val.NestedRESPData[0]
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0])    // Close the channel

		h.db.waiters[key] = h.db.waiters[key][1:] // Remove client off the queue
	}

	newLen := strconv.Itoa(len(val.NestedRESPData))
	return EncodeToRESP(
		&RESPData{
			Type: Integer,
			Data: []byte(newLen),
		})

}

func (h *CommandHandler) HandleLRange(args []*RESPData) ([]byte, bool) {
	if len(args) != 4 {
		return nil, false
	}
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	resp, ok := h.db.Get(string(args[1].Data))
	if !ok || resp.Type != Array {
		return respEmptyArr, true
	}

	arrLen := len(resp.NestedRESPData)
	start, _ := strconv.Atoi(string(args[2].Data))
	if start < 0 {
		start = arrLen + start
		start = max(0, start)
	}
	stop, _ := strconv.Atoi(string(args[3].Data))
	if stop < 0 {
		stop = arrLen + stop
		stop = max(0, stop)
	}
	stop = min(stop, arrLen-1)
	if start >= arrLen || start < 0 || stop < start || arrLen == 0 {
		return respEmptyArr, true
	}

	return EncodeToRESP(
		&RESPData{
			Type:           Array,
			NestedRESPData: resp.NestedRESPData[start : stop+1],
		})

}

func (h *CommandHandler) HandleLlen(args []*RESPData) ([]byte, bool) {
	if len(args) != 2 {
		return nil, false
	}
	arrName := string(args[1].Data)
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	arrResp, ok := h.db.Get(arrName)
	if !ok || arrResp.Type != Array {
		return EncodeToRESP(&RESPData{Type: Integer, Data: []byte("0")})
	}

	return EncodeToRESP(&RESPData{Type: Integer, Data: []byte(strconv.Itoa(len(arrResp.NestedRESPData)))})

}

func (h *CommandHandler) HandleBlpop(args []*RESPData) ([]byte, bool) {
	if len(args) != 3 {
		return nil, false
	}

	// Get the name of the array to pop from
	keyResp := args[1]
	key := string(keyResp.Data)

	// Make sure duration is a non-negative double float
	duration, err := strconv.ParseFloat(string(args[2].Data), 64)
	if err != nil || duration < 0 {
		return nil, false
	}

	h.db.mu.Lock()
	data, ok := h.db.data[key]

	// If the array exists and has elements, pop and return immediately
	if ok && data.Type == Array && len(data.NestedRESPData) > 0 {
		poppedVal := data.NestedRESPData[0]
		data.NestedRESPData = data.NestedRESPData[1:]
		h.db.mu.Unlock()
		return EncodeToRESP(&RESPData{Type: Array, NestedRESPData: []*RESPData{keyResp, poppedVal}})
	}

	// Add client to list of blocked clients on this array
	if _, ok = h.db.waiters[key]; !ok {
		h.db.waiters[key] = make([]chan *RESPData, 0)
	}
	c := make(chan *RESPData)
	h.db.waiters[key] = append(h.db.waiters[key], c)
	h.db.mu.Unlock() // Make sure to remove mutex so other clients can modify DB while this one is blocked

	// If duration is 0, block indefinitely until channel receives value
	if duration == 0.0 {
		poppedVal := <-c
		h.db.mu.Lock()
		defer h.db.mu.Unlock()
		h.db.data[key].NestedRESPData = h.db.data[key].NestedRESPData[1:] // Remove the popped element from the array
		return EncodeToRESP(&RESPData{Type: Array, NestedRESPData: []*RESPData{keyResp, poppedVal}})
	}

	// Otherwise block until channel receives value or timeout occurs
	select {
	case <-time.After(time.Duration(duration * float64(time.Second))):
		// Timeout occurred, remove client from waiters list
		h.db.mu.Lock()
		defer h.db.mu.Unlock()
		// Remove channel from waiters list
		waitChans := h.db.waiters[key]
		for i, chanVal := range waitChans {
			if chanVal == c {
				h.db.waiters[key] = append(waitChans[:i], waitChans[i+1:]...)
				break
			}
		}
		return respNullArr, true
	case poppedVal := <-c:
		// Successfully received popped value
		h.db.mu.Lock()
		defer h.db.mu.Unlock()
		h.db.data[key].NestedRESPData = h.db.data[key].NestedRESPData[1:] // Remove the popped element from the array
		return EncodeToRESP(&RESPData{Type: Array, NestedRESPData: []*RESPData{keyResp, poppedVal}})
	}

}

func (h *CommandHandler) HandleLpop(args []*RESPData) ([]byte, bool) {
	if len(args) != 2 && len(args) != 3 {
		return nil, false
	}
	arrName := string(args[1].Data)
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	arrResp, ok := h.db.Get(arrName)
	if !ok || arrResp.Type != Array {
		return respNullString, true
	}
	numToRemove := 1
	if len(args) == 3 {
		numToRemove, _ = strconv.Atoi(string(args[2].Data))
		if numToRemove < 0 {
			return nil, false
		}
		numToRemove = min(numToRemove, len(arrResp.NestedRESPData))
	}
	ret := &RESPData{Type: Array, NestedRESPData: make([]*RESPData, numToRemove)}
	for i := range numToRemove {
		elem := CloneRESP(arrResp.NestedRESPData[i])
		ret.NestedRESPData[i] = elem
	}
	if numToRemove == len(arrResp.NestedRESPData) {
		arrResp.NestedRESPData = make([]*RESPData, 0)
	} else {
		arrResp.NestedRESPData = arrResp.NestedRESPData[numToRemove:]
	}
	if numToRemove == 1 {
		return EncodeToRESP(ret.NestedRESPData[0])
	}
	return EncodeToRESP(ret)

}

func (h* CommandHandler) HandleType(args []*RESPData) ([]byte, bool) {
	if len(args) != 2 {
		return nil, false
	}
	
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	resp, ok := h.db.data[string(args[1].Data)]
	if !ok {
		return respNoneString, true
	}

	switch resp.Type {
	case SimpleString, BulkString:
		return []byte("+string\r\n"), true
	default:
		return respNoneString, true
	}
}

func (h *CommandHandler) Handle(message []byte) ([]byte, bool) {
	_, respData, success := DecodeFromRESP(message)
	if !success || respData.Type != Array {
		fmt.Println("Unable to parse RESP request")
		return nil, false
	}

	request := respData.NestedRESPData
	if len(request) == 0 {
		return nil, false
	}

	command := string(request[0].Data)
	switch strings.ToLower(command) {
	case "echo":
		return h.HandleEcho(request)
	case "ping":
		return h.HandlePing()
	case "set":
		return h.HandleSet(request)
	case "get":
		return h.HandleGet(request)
	case "rpush":
		return h.HandleRPush(request)
	case "lpush":
		return h.HandleLPush(request)
	case "lrange":
		return h.HandleLRange(request)
	case "llen":
		return h.HandleLlen(request)
	case "lpop":
		return h.HandleLpop(request)
	case "blpop":
		return h.HandleBlpop(request)
	case "type":
		return h.HandleType(request)
	default:
		return nil, false
	}
}
