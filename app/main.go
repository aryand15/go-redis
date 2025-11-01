package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	
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
	// Cleanup function after client disconnects
	defer func() {
		// Remove client from publisher, subscriber, and receiver lists
		handler.db.mu.Lock()
		if _, ok := handler.db.publishers[conn]; ok {
			for topic := range handler.db.publishers[conn].set {
        		handler.db.subscribers[topic].Remove(conn)
			}
		}
		
		delete(handler.db.publishers, conn)
		delete(handler.db.receivers, conn)



		handler.db.mu.Unlock()

		// Close TCP connection
		conn.Close()
	}()
	
	inTransaction := false
	inSubscribeMode := false

	clientInput := make(chan []byte)

	go func() {
		buf := make([]byte, bufferSize)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				close(clientInput)
				return
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			clientInput <- data

		}
	}()

	for {
		
		var response []byte
		ok := false

		if inSubscribeMode {

			select {
			case message, sentOk := <-clientInput:

				if !sentOk {	// Error reading client input buffer
                	return 
            	}
				_, respData, success := DecodeFromRESP(message)
		
				if !success || respData.Type != Array || len(respData.ListRESPData) == 0 {
					fmt.Println("Unable to parse RESP request")
					return
				}

				firstWord := strings.ToLower(string(respData.ListRESPData[0].Data))
				response, ok = handler.HandleSubscribeMode(respData, conn)
				if ok && firstWord == "quit" {
					inSubscribeMode = false
				}
			case publisherInput, sentOk := <-handler.db.receivers[conn]:
				if !sentOk {	// Channel is closed
					return
				}
				response = []byte(publisherInput)
				ok = true

			}
		} else {

			message, sentOk := <-clientInput
			if !sentOk {
				return
			}

			_, respData, success := DecodeFromRESP(message)
			
			if !success || respData.Type != Array || len(respData.ListRESPData) == 0 {
				fmt.Println("Unable to parse RESP request")
				return
			}

			firstWord := strings.ToLower(string(respData.ListRESPData[0].Data))

			if firstWord == "exec" || firstWord == "discard" {
				inTransaction = false
			}

			response, ok = handler.Handle(respData, conn, inTransaction)


			if firstWord == "multi" {
				inTransaction = !inTransaction
			} else if ok && !inSubscribeMode && firstWord == "subscribe" {
				inSubscribeMode = true
			}



		}

		
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