package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

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
		command := string(respData.NestedRESPData[0].Data)
		var output []byte
		switch strings.ToLower(command) {
		case "echo":
			output, success = EncodeToRESP(RESPData{Type:BulkString, Data:respData.NestedRESPData[1].Data})
			if (!success) {
				fmt.Println("Error encoding to RESP.")
				return
			}
		case "ping":
			output = []byte("+PONG\r\n")
		default:
			output = []byte("I have never seen this command before.")

		}
		_, err = conn.Write(output)
		if err != nil {
			fmt.Println("Error sending PONG response: ", err.Error())
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
