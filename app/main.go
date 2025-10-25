package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

type DB struct {
	data map[string]RESPData
	mu sync.Mutex
}

func (db *DB) Set(key string, val RESPData) {
	db.mu.Lock()
	db.data[key] = val
	db.mu.Unlock()
}

func (db *DB) Get(key string) (RESPData, bool) {
	db.mu.Lock()
	val, ok := db.data[key]
	db.mu.Unlock()
	return val, ok 
}

func NewDB() *DB {
	return &(DB{ data: make(map[string]RESPData) })
}

var globalDB *DB = NewDB()

func handleConn(conn net.Conn) {
	// Continuously read messages from connection.

	defer conn.Close()
	buf := make([]byte, 1024) // Buffer to hold incoming commands

	for {
		// Read the raw RESP message
		n, err := conn.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error reading response: ", err.Error())
			return
		}
		encodedMessage := buf[:n]

		// Parse RESP and convert into readable commands
		_, respData, success := DecodeFromRESP(encodedMessage)
		if !success || respData.Type != Array {
			fmt.Println("Unable to parse RESP request")
			continue
		}

		// Extract the first word to get the command
		request := respData.NestedRESPData
		command := string(request[0].Data)
		var output []byte

		// Handle the different commands
		switch strings.ToLower(command) {
		case "echo":
			output, success = EncodeToRESP(RESPData{Type:BulkString, Data:respData.NestedRESPData[1].Data})
			if (!success) {
				fmt.Println("Error encoding to RESP.")
				return
			}
		case "ping":
			output = []byte("+PONG\r\n")
		case "set":
			key := string(request[1].Data)
			val := request[2]
			globalDB.Set(key, val)
		case "get":
			key := string(request[1].Data)
			val, ok := globalDB.Get(key)
			if !ok {
				output = []byte("$-1\r\n")
			} else {
				output, _ = EncodeToRESP(val)
			}

		default:
			output = []byte("I have never seen this command before.")

		}
		_, err = conn.Write(output)
		if err != nil {
			fmt.Println("Error sending response: ", err.Error())
			return
		}
		
	}
}

func main() {

	// Create a TCP connection, listening on port 6379 for incoming connections.
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// Listen for and handle concurrent connections.
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}

}
