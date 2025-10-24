package main

import (
	"fmt"
	"net"
	"os"
	"io"
)

func handleConn(conn net.Conn) {
	// Continuously read messages from connection and respond "PONG" to each one.
	defer conn.Close()
	pong := []byte("+PONG\r\n")
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error reading response: ", err.Error())
			return
		}

		_, err = conn.Write(pong)
		if err != nil {
			fmt.Println("Error sending PONG response: ", err.Error())
			return
		}
	}
}

func main() {

	// Create a TCP connection, listening on port 6379.
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
