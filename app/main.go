package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

// DB represents an in-memory key-value store
type DB struct {
	data map[string]RESPData
	mu   sync.Mutex // Prevents concurrency issues
}

func (db *DB) Set(key string, val RESPData) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = val
}

func (db *DB) Get(key string) (RESPData, bool) {
	db.mu.Lock()
	val, ok := db.data[key]
	db.mu.Unlock()
	return val, ok
}

func NewDB() *DB {
	return &DB{data: make(map[string]RESPData)}
}

const (
	defaultPort = ":6379"
	bufferSize  = 1024
)

func main() {
	db := NewDB()
	handler := NewCommandHandler(db)

	if err := startServer(handler); err != nil {
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	}
}

func startServer(handler *CommandHandler) error {
	l, err := net.Listen("tcp", defaultPort)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", defaultPort, err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %v", err)
		}
		go handleConn(conn, handler)
	}
}

func handleConn(conn net.Conn, handler *CommandHandler) {
	defer conn.Close()
	buf := make([]byte, bufferSize)

	for {
		n, err := conn.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error reading from connection: %v\n", err)
			return
		}

		response, ok := handler.Handle(buf[:n])
		if !ok {
			fmt.Println("Error handling message")
			return
		}

		if _, err := conn.Write(response); err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			return
		}
	}
}
