package server

import (
	"fmt"
	"log"
	"net"

	"github.com/aryand15/go-redis/app/client"
	"github.com/aryand15/go-redis/app/commands"
	"github.com/aryand15/go-redis/resp"
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
		client := client.NewClient(&conn)
		go handleConn(client, handler)
	}
}

func handleConn(client *client.Client, handler *commands.CommandHandler) {
	log.Printf("New connection from %s", client.Id())

	// Since reading is a blocking operation, we spawn a goroutine to handle client commands
	// This allows us to handle pub/sub commands so we can simultaneously listen to publisher messages as well as client input in subscribe mode
	// Otherwise, we could only listen to one message stream or the other, but not both.
	go readInputFromClient(client, handler)

	for {

		var res *resp.RESPData
		var cmdProcessErr error

		// Handle both client input and publisher messages when in subscribe mode, each with different channels
		// Handle whichever comes first
		select {

		case message, sentOk := <-client.ClientInputChan: // Case 1: Client input comes first

			if !sentOk { // Error reading client input buffer
				return
			}

			// Deserialize client message RESP, handling any issues if need be
			respData, decodeErr := resp.DecodeFromRESP(message)
			if decodeErr != nil || respData.Type != resp.Array || len(respData.ListRESPData) == 0 {
				fmt.Printf("Internal server error: Unable to parse RESP request: %s\n", string(message))
				return
			}

			// Handle message and receive response
			res, cmdProcessErr = handler.Handle(respData, client)

			// If command failed due to client disconnection, exit without trying to write
			if cmdProcessErr != nil && cmdProcessErr.Error() == "client disconnected" {
				return
			}

		case publisherInput, sentOk := <-client.PubSubChan: // Case 2: Publisher messages come first
			if !sentOk { // Channel is closed
				return
			}

			res = resp.ConvertBulkStringToRESP(publisherInput)
		}

		// Allocate buffer to hold response to send back to client
		var response []byte

		// If no error while handling command, convert RESP data structure into a RESP string to send back to client
		// Otherwise, convert the error string into a RESP string
		if cmdProcessErr != nil {
			response, _ = resp.ConvertSimpleErrorToRESP(cmdProcessErr.Error()).EncodeToRESP()
		} else {
			response, _ = res.EncodeToRESP()
		}

		// Write response back to client, handling any errors
		if _, err := client.GetConn().Write(response); err != nil {
			fmt.Printf("Internal Server Error: error writing response: %v\n", err)
			return
		}
	}
}

// handleDisconnect removes any data associated with this client from the datastore on disconnect,
// including removing it from client queues blocked on BLPOP, sets of clients blocked on XREAD, and other blocking operations
// as necessary.
func handleDisconnect(c *client.Client, handler *commands.CommandHandler) {
	close(c.ClientInputChan)
	db := handler.GetDB()
	db.Lock()

	// If client is in subscribe mode, remove client from subscription list of every publisher
	// and close its pub-sub channel
	if c.IsInSubscribeMode() {
		clientPubs, _ := c.GetPublishers()
		for pub := range clientPubs.Items() {
			db.RemoveSubscriber(c, pub)
		}
		close(c.PubSubChan)

		// Otherwise remove from BLPOP and XREAD waiters
	} else if !c.IsInTransactionMode() {
		xreadData, _ := c.GetXREADIds()
		for streamKey, idToChan := range xreadData {
			for id := range idToChan {
				db.RemoveXREADIDWaiter(streamKey, id, c)
			}
		}

		blpopData, _ := c.GetBlpopKeys()
		for key := range blpopData.Items() {
			db.RemoveBLPOPWaiter(key, c)
		}
	}

	db.Unlock()

	// Close BlockOpChan after removing from waiters to prevent race condition
	close(c.BlockOpChan)

	// Close TCP connection
	c.CloseConn()

	log.Printf("Connection closed: %s", c.Id())
}

// Function that continously reads messages from connection and sends to a receiving channel
func readInputFromClient(client *client.Client, handler *commands.CommandHandler) {
	defer handleDisconnect(client, handler)
	buf := make([]byte, bufferSize)
	for {
		n, err := client.GetConn().Read(buf)
		if err != nil {
			return
		}
		msg := buf[:n]
		data := make([]byte, n)
		copy(data, msg)
		client.ClientInputChan <- data

	}
}
