package main

import (
	"fmt"
	"strings"
	"strconv"
	"time"
)

// Common RESP responses
var (
	respOK   = []byte("+OK\r\n")
	respPong = []byte("+PONG\r\n")
	respNull = []byte("$-1\r\n")
	respEmptyArr = []byte("*0\r\n")
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
	} else if timeOption := string(args[3].Data); (timeOption != "PX" && timeOption != "EX") {
		return nil, false
	} else if duration, err := strconv.Atoi(string(args[4].Data)); (err != nil || duration < 0) {
		return nil, false
	} else if timeOption == "EX" {
		h.db.TimedSet(key, args[2], time.Duration(duration) * time.Second)
	} else if timeOption == "PX" {
		h.db.TimedSet(key, args[2], time.Duration(duration) * time.Millisecond)
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
		return respNull, true
	}
	return EncodeToRESP(val)
}

func (h *CommandHandler) HandleRPush(args []*RESPData) ([]byte, bool) {
	if len(args) < 3 {
		return nil, false
	}
	key := string(args[1].Data)
	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	val, ok := h.db.Get(key)
	if !ok || val.Type != Array {
		val = &RESPData{Type: Array, NestedRESPData: make([]*RESPData, 0)}
		h.db.Set(key, val)
	}

	for i := 2; i < len(args); i++ {
		val.NestedRESPData = append(val.NestedRESPData, CloneRESP(args[i]))
	}

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
	newArr := make([]*RESPData, len(val.NestedRESPData) + len(args)-2)
	for i := 0; i < len(args)-2; i++ {
		newArr[i] = CloneRESP(args[len(args)-1-i])
	}
	for i := len(args)-2; i < len(newArr); i++ {
		newArr[i] = CloneRESP(val.NestedRESPData[i-len(args)+2])
	}
	val.NestedRESPData = newArr

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	firstElem := string(val.NestedRESPData[0].Data)
	if waitChans, ok := h.db.waiters[firstElem]; ok {
		popped := val.NestedRESPData[0]
		val.NestedRESPData = val.NestedRESPData[1:] // Remove the popped element from the array
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0]) // Close the channel
		h.db.waiters[firstElem] = h.db.waiters[firstElem][1:] // Remove client off the queue
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
			Type: Array,
			NestedRESPData: resp.NestedRESPData[start:stop+1],
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
	if !ok || arrResp.Type != Array{
		return EncodeToRESP(&RESPData{Type: Integer, Data: []byte("0")})
	}

	return EncodeToRESP(&RESPData{Type: Integer, Data: []byte(strconv.Itoa(len(arrResp.NestedRESPData)))})


}

func (h *CommandHandler) HandleBlpop(args []*RESPData) ([]byte, bool) {
	if len(args) != 3 {
		return nil, false
	}
	keyResp := args[1]
	key := string(keyResp.Data)
	//duration := args[2]
	h.db.mu.Lock()
	data, ok := h.db.data[key]
	if ok && data.Type == Array && len(data.NestedRESPData) > 0 {
		// If the array exists and has elements, pop and return immediately
		poppedVal := data.NestedRESPData[0]
		data.NestedRESPData = data.NestedRESPData[1:]
		h.db.mu.Unlock()
		return EncodeToRESP(&RESPData{Type: Array, NestedRESPData: []*RESPData{keyResp, poppedVal}})
	}
	_, ok = h.db.waiters[key]
	if !ok {
		h.db.waiters[key] = make([]chan *RESPData, 0)
	}
	c := make(chan *RESPData)
	h.db.waiters[key] = append(h.db.waiters[key], c)
	h.db.mu.Unlock()

	// Block until channel received value (add duration later)
	poppedVal := <-c
	return EncodeToRESP(&RESPData{Type: Array, NestedRESPData: []*RESPData{keyResp, poppedVal}})


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
		return respNull, true
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
	if (numToRemove == 1) {
		return EncodeToRESP(ret.NestedRESPData[0])
	}
	return EncodeToRESP(ret)


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
	default:
		return nil, false
	}
}