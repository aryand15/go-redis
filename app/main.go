package main

import (
	"fmt"
	"net"
	"os"
)


func main() {

	// Create a TCP connection, listening on port 6379.
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	// Block until another host attempts connection with the listener.
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	// Attempt to send PONG by default when a connection is received.
	pong := []byte("+PONG\r\n")
	_, err = conn.Write(pong)
	if err != nil {
		fmt.Println("Error sending PONG response: ", err.Error())
		os.Exit(1)
	}
}
