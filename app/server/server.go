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

		var res *resp.RESPData
		var err error

		if inSubscribeMode {
			receiver, _ := handler.GetDB().GetReceiver(conn)
			select {
			case message, sentOk := <-clientInput:

				if !sentOk { // Error reading client input buffer
					return
				}
				_, respData, success := resp.DecodeFromRESP(message)

				if !success || respData.Type != resp.Array || len(respData.ListRESPData) == 0 {
					fmt.Printf("Internal server error: Unable to parse RESP request: %s\n", string(message))
					return
				}

				firstWord := strings.ToLower(string(respData.ListRESPData[0].Data))
				res, err = handler.HandleSubscribeMode(respData, conn)
				if err == nil && firstWord == "quit" {
					inSubscribeMode = false
				}
			case publisherInput, sentOk := <-receiver:
				if !sentOk { // Channel is closed
					return
				}

				res = resp.ConvertBulkStringToRESP(publisherInput)
			}
		} else {

			message, sentOk := <-clientInput
			if !sentOk {
				return
			}

			_, respData, success := resp.DecodeFromRESP(message)

			if !success || respData.Type != resp.Array || len(respData.ListRESPData) == 0 {
				fmt.Printf("Internal server error: Unable to parse RESP request: %s\n", string(message))
				return
			}

			firstWord := strings.ToLower(string(respData.ListRESPData[0].Data))

			if firstWord == "exec" || firstWord == "discard" {
				inTransaction = false
			}

			res, err = handler.Handle(respData, conn, inTransaction)

			if firstWord == "multi" {
				inTransaction = !inTransaction
			} else if err == nil && !inSubscribeMode && firstWord == "subscribe" {
				inSubscribeMode = true
			}

		}

		var response []byte
		if err != nil {
			response, _ = resp.ConvertSimpleErrorToRESP(err.Error()).EncodeToRESP()
		} else {
			response, _ = res.EncodeToRESP()
		}

		if _, err := conn.Write(response); err != nil {
			fmt.Printf("Internal Server Error: error writing response: %v\n", err)
			return
		}
	}
}
