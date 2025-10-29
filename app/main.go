package main

import (
	"fmt"
	"io"
	"net"
	"os"
)



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

		var response []byte
		ok := false
		message := buf[:n]


		handler.db.mu.Lock()

		_, respData, success := DecodeFromRESP(message)
		if !success || respData.Type != Array {
			fmt.Println("Unable to parse RESP request")
		} else if len(respData.ListRESPData) == 0 {
			response, ok = nil, false
		
		// Handle MULTI command
		} else if n == 1 && string(respData.ListRESPData[0].Data) == "multi" {
			fmt.Println("In the outer if statement")
			// Create new transaction if nonexistent
			if _, ok = handler.db.transactions[conn]; !ok {
				fmt.Println("In the important if statement")
				handler.db.transactions[conn] = make([][]byte, 0)
				response, ok = []byte("+OK\r\n"), true
			
			// Cannot call MULTI while already in a transaction
			} else {
				response, ok = nil, false
			}
			
			handler.db.mu.Unlock()

		// If command is in a transaction and is NOT "exec", queue it instead of actually handling it
		} else if _, ok = handler.db.transactions[conn]; ok && string(respData.ListRESPData[0].Data) != "exec" {
			handler.db.transactions[conn] = append(handler.db.transactions[conn], message)
			handler.db.mu.Unlock()
			response = []byte("+QUEUED\r\n")
		
		// Otherwise, proceed as normal and handle the message
		} else {
			handler.db.mu.Unlock()
			response, ok = handler.Handle(respData, conn)
		}

		fmt.Println("OK:", ok)
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