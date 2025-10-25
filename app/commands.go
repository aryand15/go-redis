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
	_, ok := h.db.Get(key)
	if !ok {
		h.db.Set(key, &RESPData{Type: Array, NestedRESPData: make([]*RESPData, 0)})
	}

	val, _ := h.db.Get(key)

	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	val.NestedRESPData = append(val.NestedRESPData, args[2:]...)
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

	resp, ok := h.db.Get(string(args[1].Data))
	if !ok {
		return respEmptyArr, true
	}

	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	start, _ := strconv.Atoi(string(args[2].Data))
	fmt.Println(start)
	stop, _ := strconv.Atoi(string(args[3].Data))
	fmt.Println(stop)
	arrLen := len(resp.NestedRESPData)
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
	case "lrange":
		return h.HandleLRange(request)
	default:
		return nil, false
	}
}