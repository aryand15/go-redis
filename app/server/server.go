package server

import (
	"strings"
	"github.com/aryand15/go-redis/app/commands"
	"github.com/aryand15/go-redis/resp"
	"fmt"
	"net"
	"log"
)

const (
	defaultPort = ":6379"
	bufferSize  = 1024
)

func StartServer(handler *commands.CommandHandler) error {
	l, err := net.Listen("tcp", defaultPort)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", defaultPort, err)
	}
	log.Println("Server started!")
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return fmt.Errorf("error accepting connection: %v", err)
		}
		go handleConn(conn, handler)
	}
}

func handleConn(conn net.Conn, handler *commands.CommandHandler) {
	log.Printf("New connection from %s", conn.RemoteAddr())

	// Cleanup function after client disconnects
	defer func() {
		// Remove client from subscription list of every publisher
		handler.GetDB().Lock()
		if pubs, ok := handler.GetDB().GetPublishers(conn); ok {
			for pub := range pubs.Items() {
				handler.GetDB().RemoveSubscriberFromPublisher(pub, conn)
			}
		}

		// Remove the client from map of all subscribers and remove its receiving channel
		handler.GetDB().DeleteSubscriber(conn)

		// TODO: remove from BLPOP waiters, XREAD ID and XREAD ALL waiters

		handler.GetDB().Unlock()

		// Close TCP connection
		conn.Close()

		log.Printf("Connection closed: %s", conn.RemoteAddr())
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

		var response = make([]byte, 0)
		ok := false

		if inSubscribeMode {
			receiver, _ := handler.GetDB().GetReceiver(conn)
			select {
			case message, sentOk := <-clientInput:

				if !sentOk { // Error reading client input buffer
					return
				}
				_, respData, success := resp.DecodeFromRESP(message)

				if !success || respData.Type != resp.Array || len(respData.ListRESPData) == 0 {
					fmt.Println("Unable to parse RESP request")
					return
				}

				firstWord := strings.ToLower(string(respData.ListRESPData[0].Data))
				response, ok = handler.HandleSubscribeMode(respData, conn)
				if ok && firstWord == "quit" {
					inSubscribeMode = false
				}
			case publisherInput, sentOk := <-receiver:
				if !sentOk { // Channel is closed
					return
				}

				response, ok = resp.ConvertBulkStringToRESP(publisherInput).EncodeToRESP()
			}
		} else {

			message, sentOk := <-clientInput
			if !sentOk {
				return
			}

			_, respData, success := resp.DecodeFromRESP(message)

			if !success || respData.Type != resp.Array || len(respData.ListRESPData) == 0 {
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
