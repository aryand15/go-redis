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
	// Start a listener on port (6379 by default) via TCP
	l, err := net.Listen("tcp", defaultPort)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %v", defaultPort, err)
	}

	log.Println("Server started!")

	// Make sure to stop listener when server is terminated
	defer l.Close() 

	// Continually accept and handle connections from clients
	for {
		conn, err := l.Accept() // blocks until another connection occurs
		if err != nil {
			return fmt.Errorf("error accepting connection: %v", err)
		}
		go handleConn(conn, handler)
	}
}

func handleConn(conn net.Conn, handler *commands.CommandHandler) {
	log.Printf("New connection from %s", conn.RemoteAddr())

	// Cleanup function after client disconnects
	defer handleDisconnect(conn, handler)

	isInTransaction := false
	inSubscribeMode := false

	// Since reading is a blocking operation, we spawn a goroutine to handle client commands
	// This allows us to handle pub/sub commands so we can simultaneously listen to publisher messages as well as client input in subscribe mode
	// Otherwise, we could only listen to one message stream or the other, but not both.
	clientInput := make(chan []byte)
	go readInputFromConn(conn, clientInput)

	for {

		var res *resp.RESPData
		var err error

		// Dedicated channel for this client to receive updates from its publishers (nil if not in subscribe mode)
		var receiver <-chan string
		if inSubscribeMode {
			receiver, _ = handler.GetDB().GetReceiver(conn)
		}

		// Handle both client input and publisher messages when in subscribe mode, each with different channels
		// Handle whichever comes first
		select {
		
		case message, sentOk := <-clientInput:	// Case 1: Client input comes first

			if !sentOk { // Error reading client input buffer
				return
			}

			// Deserialize client message RESP
			respData, firstWord, success := parseClientInput(message)
			if !success {
				return
			}

			// Handle the command if made in subscribe mode	
			// If the user typed QUIT while in subscribe mode, remove them from subscribe mode
			if inSubscribeMode {
				res, err = handler.HandleSubscribeMode(respData, conn)
				if err == nil && firstWord == "quit" {
					inSubscribeMode = false
				}
			
			// Otherwise, handle transaction modes and normal modes
			} else {
				if firstWord == "exec" || firstWord == "discard" {
					isInTransaction = false
				}

				res, err = handler.Handle(respData, conn, isInTransaction)

				if firstWord == "multi" {
					isInTransaction = !isInTransaction
				} else if err == nil && !inSubscribeMode && firstWord == "subscribe" {
					inSubscribeMode = true
				}
			}

		case publisherInput, sentOk := <-receiver:	// Case 2: Publisher messages come first
			if !sentOk { // Channel is closed
				return
			}

			res = resp.ConvertBulkStringToRESP(publisherInput)
		}
		
		
		// Allocate buffer to hold response to send back to client
		var response []byte

		// If no error while handling command, convert RESP data structure into a RESP string to send back to client
		// Otherwise, convert the error string into a RESP string
		if err != nil {
			response, _ = resp.ConvertSimpleErrorToRESP(err.Error()).EncodeToRESP()
		} else {
			response, _ = res.EncodeToRESP()
		}

		// Write response back to client, handling any errors
		if _, err := conn.Write(response); err != nil {
			fmt.Printf("Internal Server Error: error writing response: %v\n", err)
			return
		}
	}
}


// Deserializes a RESP-serialized client message.
// Returns the deserialized message, the command (first word), and whether the parsing was successful
func parseClientInput(message []byte) (*resp.RESPData, string, bool) {
	_, respData, success := resp.DecodeFromRESP(message)
	// Error: Either the string was improperly serialized, or it wasn't an array type, or it was an empty array
	if !success || respData.Type != resp.Array || len(respData.ListRESPData) == 0 {
		fmt.Printf("Internal server error: Unable to parse RESP request: %s\n", string(message))
		return nil, "", false
	}
	firstWord := strings.ToLower(string(respData.ListRESPData[0].Data))
    return respData, firstWord, true

}

// Removes any data associated with this client from the datastore on disconnect
func handleDisconnect(conn net.Conn, handler *commands.CommandHandler) {
	// Remove client from subscription list of every publisher
	db := handler.GetDB()
	db.Lock()
	if pubs, ok := handler.GetDB().GetPublishers(conn); ok {
		for pub := range pubs.Items() {
			db.RemoveSubscriberFromPublisher(pub, conn)
		}
	}

	// Remove the client from map of all subscribers and remove its receiving channel
	db.DeleteSubscriber(conn)

	// TODO: remove from BLPOP waiters, XREAD ID and XREAD ALL waiters

	db.Unlock()

	// Close TCP connection
	conn.Close()

	log.Printf("Connection closed: %s", conn.RemoteAddr())
}

// Function that continously reads messages from connection and sends to a receiving channel
func readInputFromConn(conn net.Conn, receiverChan chan<- []byte) {
	buf := make([]byte, bufferSize)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			close(receiverChan)
			return
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		receiverChan <- data

	}
}
