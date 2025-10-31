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
		
		if !success || respData.Type != Array || len(respData.ListRESPData) == 0 {
			fmt.Println("Unable to parse RESP request")
		}

		response, ok = handler.Handle(respData, conn)

		if !ok {
			fmt.Println("Error handling message")

			// Remove connection from transactions map if it exists
			handler.db.mu.Lock()
			delete(handler.db.transactions, conn)
			handler.db.mu.Unlock()
			return
		}

		if _, err := conn.Write(response); err != nil {
			fmt.Printf("Error writing response: %v\n", err)
			return
		}
	}
}