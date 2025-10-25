package main

import (
	"fmt"
)

// Common RESP responses
var (
	respOK   = []byte("+OK\r\n")
	respPong = []byte("+PONG\r\n")
	respNull = []byte("$-1\r\n")
)

type CommandHandler struct {
	db *DB
}

func NewCommandHandler(db *DB) *CommandHandler {
	return &CommandHandler{db: db}
}

func (h *CommandHandler) HandleEcho(args []RESPData) ([]byte, bool) {
	if len(args) < 2 {
		return nil, false
	}
	return EncodeToRESP(RESPData{Type: BulkString, Data: args[1].Data})
}

func (h *CommandHandler) HandlePing() ([]byte, bool) {
	return respPong, true
}

func (h *CommandHandler) HandleSet(args []RESPData) ([]byte, bool) {
	if len(args) < 3 {
		return nil, false
	}
	key := string(args[1].Data)
	h.db.Set(key, args[2])
	return respOK, true
}

func (h *CommandHandler) HandleGet(args []RESPData) ([]byte, bool) {
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
	switch command {
	case "echo", "ECHO":
		return h.HandleEcho(request)
	case "ping", "PING":
		return h.HandlePing()
	case "set", "SET":
		return h.HandleSet(request)
	case "get", "GET":
		return h.HandleGet(request)
	default:
		return nil, false
	}
}