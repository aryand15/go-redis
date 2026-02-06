package client

import (
	"fmt"
	"net"

	"github.com/aryand15/go-redis/app/utils"
	"github.com/aryand15/go-redis/resp"
)

type Client struct {
	transaction            *utils.Deque[*resp.RESPData] // the list of commands that the user has made so far under their current transaction
	isInTransactionMode    bool                         // represents whether the client is currently adding commands to a transaction or not
	isExecutingTransaction bool                         // represents whether the client is currently executing a transaction

	isInSubscribeMode bool               // whether the client is in subscribe mode or not
	PubSubChan       chan string        // a channel for the client (subscriber) to receive messages from publishers asynchronously
	publishers        *utils.Set[string] // the set of all publishers that the client is currently subscribed to

	conn      *net.Conn   // the client's unique tcp connection
	ClientInputChan chan []byte // a channel for the client to send messages (commands) to the server

	BlockOpChan chan *resp.RESPData // a channel for the client to receive data from blocking operations (BLPOP, XREAD, etc.)

	blpopKeys *utils.Set[string]            // the set of all list keys that the client is currently blocked on for BLPOP
	xreadIds  map[string]map[string]chan *resp.RESPData
	//xreadIds  map[string]*utils.Set[string] // the map of stream keys -> ids that the client is currently blocked on for XREAD

}

// Id returns a unique identifier for this client (their remote address in this case).
func (c *Client) Id() string {
	return (*c.conn).RemoteAddr().String()
}

// CloseConn closes the TCP connection between itself and the server.
func (c *Client) CloseConn() {
	(*c.conn).Close()
}

// GetConn returns the TCP connection object between itself and the server.
func (c *Client) GetConn() net.Conn {
	return *c.conn
}

// IsInTransactionMode returns whether or not the client is in transaction mode.
func (c *Client) IsInTransactionMode() bool {
	return c.isInTransactionMode
}

// GetTransaction returns the list of commands the client has made from the db if they are currently performing a transaction;
// nil if they are not in a transaction (empty array could mean that they just haven't made any commands yet).
func (c *Client) GetTransaction() (*utils.Deque[*resp.RESPData], error) {
	if !c.IsInTransactionMode() {
		return nil, fmt.Errorf("The client is not in transaction mode")
	}
	return c.transaction, nil
}

// IsExecutingTransaction returns whether or not the client is currently executing a transaction
func (c *Client) IsExecutingTransaction() bool {
	return c.isExecutingTransaction
}

// EndTransaction deletes all transaction data for the specified client, updating its state 
// as necessary if it is buliding a transaction or executing it. 
// It returns an error if there is no transaction data for this client to delete.
func (c *Client) EndTransaction() error {
	if !c.IsInTransactionMode() && !c.IsExecutingTransaction() {
		return fmt.Errorf("Transaction data does not exist for this client")
	}
	c.transaction.Clear()
	c.isInTransactionMode = false
	c.isExecutingTransaction = false
	return nil
}

// PopFromTransaction returns the next command in the list of commands in the client's current 
// transaction. If the list of commands is empty, or if the client isn't building or executing a transaction 
// to beign with, it returns an error.
func (c *Client) PopFromTransaction() (*resp.RESPData, error) {
	if !c.IsInTransactionMode() && !c.IsExecutingTransaction() {
		return nil, fmt.Errorf("Transaction data does not exist for this client")
	}
	command, err := c.transaction.PopLeft()
	if err != nil {
		return nil, err
	}
	if !c.isExecutingTransaction {
		c.isExecutingTransaction = true
	}
	if c.isInTransactionMode {
		c.isInTransactionMode = false
	}
	return command, nil
}

// StartTransaction starts a transaction for this client.
// It returns an error if transaction data already exists for the client or if the client is in subscribe mode.
func (c *Client) StartTransaction() error {
	if c.IsInTransactionMode() {
		return fmt.Errorf("Transaction data already exists for this client")
	}
	if c.IsExecutingTransaction() {
		return fmt.Errorf("Cannot start new transaction while already executing one")
	}
	if c.IsInSubscribeMode() {
		return fmt.Errorf("Cannot start transaction while in subscribe mode")
	}

	c.transaction.Clear()
	c.isInTransactionMode = true
	return nil
}

// AddToTransaction adds a client request to their existing transaction.
// It returns an error if transaction data does not already exist for this client.
func (c *Client) AddToTransaction(request *resp.RESPData) error {
	if !c.IsInTransactionMode() {
		return fmt.Errorf("Transaction data does not exist for this client")
	}
	if c.IsExecutingTransaction() {
		return fmt.Errorf("Cannot update a transaction while already executing one")
	}
	c.transaction.PushRight(request)
	return nil
}

// IsInSubscribeMode returns whether or not the client is in subscribe mode.
func (c *Client) IsInSubscribeMode() bool {
	return c.isInSubscribeMode
}

// SetSubscribeMode turns subscribe mode on or off.
func (c *Client) SetSubscribeMode(on bool) error {
	if c.IsInTransactionMode() || c.IsExecutingTransaction() {
		return fmt.Errorf("cannot toggle subscribe mode while client is conducting transaction")
	}
	// do nothing if it's already on or already off
	if c.IsInSubscribeMode() == on {
		return nil
	}

	c.isInSubscribeMode = on
	if on {
		c.PubSubChan = make(chan string)
	} else {
		close(c.PubSubChan)
		c.PubSubChan = nil
	}
	
	c.publishers.Clear()
	return nil
}

// GetPublishers returns the set of channels that the client is currently subscribed to. Returns an error if
// client is not in subscribe mode.
func (c *Client) GetPublishers() (*utils.Set[string], error) {
	if !c.IsInSubscribeMode() {
		return nil, fmt.Errorf("client is not in subscribe mode")
	}
	return c.publishers, nil
}

// AddPublisher adds the given client to the subscriber list for this particular channel named pub.
// If the client is already subscribed to the publisher, it is a no-op. It returns an error if the client is
// not in subscribe mode.
func (c *Client) AddPublisher(pub string) error {
	if !c.IsInSubscribeMode() {
		return fmt.Errorf("client is not in subscribe mode")
	}
	c.publishers.Add(pub)
	return nil
}

// RemovePublisher removes the given publisher channel from the subscription list for this particular client.
// If the client's publisher list doesn't exist to begin with, it is a no-op. It returns an error if the client is
// not in subscribe mode.
func (c *Client) RemovePublisher(pub string) error {
	if !c.IsInSubscribeMode() {
		return fmt.Errorf("client is not in subscribe mode")
	}
	c.publishers.Remove(pub)
	return nil
}

// GetBlpopKeys returns all the keys that the client is currently blocked on for BLPOP.
// Returns an error if the client is in subscribe mode or is conducting a transaction.
func (c *Client) GetBlpopKeys() (*utils.Set[string], error) {
	if c.IsInSubscribeMode() || c.IsInTransactionMode() {
		return nil, fmt.Errorf("cannot get blpop keys when client is in transaction or subscribe mode.")
	}
	return c.blpopKeys, nil
}

// AddBlpopKey adds a key to the list of all keys that the client is currently blocked on for BLPOP. It is a no-op if
// the client is already blocked on BLPOP for this specific key.
// Returns an error if the client is in subscribe mode or is conducting a transaction.
func (c *Client) AddBlpopKey(key string) error {
	if c.IsInSubscribeMode() || c.IsInTransactionMode() {
		return fmt.Errorf("cannot add blpop key while client is in transaction or subscribe mode.")
	}
	c.blpopKeys.Add(key)
	return nil
}

// RemoveBlpopKey removes the specified key to the list of all keys that the client is currently blocked on for BLPOP.
// Returns an error if the client is in subscribe mode or is conducting a transaction.
// It is a no-op if the key does not exist in the client's blpop key list.
func (c *Client) RemoveBlpopKey(key string) error {
	if c.IsInSubscribeMode() || c.IsInTransactionMode() {
		return fmt.Errorf("cannot remove blpop key while client is in transaction or subscribe mode.")
	}
	c.blpopKeys.Remove(key)
	return nil
}

// GetXREADIds returns a map of stream keys to the ids that the client is blocked on for XREAD for that particular stream.
// Returns an error if the client is in subscribe mode or is conducting a transaction.
func (c *Client) GetXREADIds() (map[string]map[string]chan *resp.RESPData, error) {
	if c.IsInSubscribeMode() || c.IsInTransactionMode() {
		return nil, fmt.Errorf("cannot access xread keys and ids while client is in transaction or subscribe mode.")
	}
	return c.xreadIds, nil
}

// AddXREADId takes a stream name and a stream ID, and marks the client as ready to receive an update from the
// blocking XREAD call when an entry is added to the specified stream whose ID is greater than the given id.
// Returns an error if the client is in subscribe mode or is conducting a transaction. It's a no-op if the client
// is already waiting on that specific id. Do not input an invalid key that doesn't exist in the database or an invalid ID.
func (c *Client) AddXREADId(sname string, id string) error {
	if c.IsInSubscribeMode() || c.IsInTransactionMode() {
		return fmt.Errorf("cannot update xread keys and ids while client is in transaction or subscribe mode.")
	}

	if _, ok := c.xreadIds[sname]; !ok {
		c.xreadIds[sname] = make(map[string]chan *resp.RESPData)
	}
	if _, ok := c.xreadIds[sname][id]; !ok {
		c.xreadIds[sname][id] = make(chan *resp.RESPData)
	}
	
	return nil
}

// RemoveXREADID takes a stream name and a stream ID, and marks the client as not listening
// for an entry to be added to the specified stream whose ID is greater than the given id.
// It returns an error if it is not already listening to the specific id within the specific stream.
// Do not input an invalid stream name or id.
func (c *Client) RemoveXREADID(sname string, id string) error {
	if c.IsInSubscribeMode() || c.IsInTransactionMode() {
		return fmt.Errorf("cannot delete xread keys and ids while client is in transaction or subscribe mode.")
	}
	
	if _, ok := c.xreadIds[sname]; !ok {
		return fmt.Errorf("client is not blocked on XREAD for stream '%s'", sname)
	}

	if _, idOk := c.xreadIds[sname][id]; !idOk {
		return fmt.Errorf("client is not blocked on XREAD for stream '%s' on id '%s", sname, id)
	}

	close(c.xreadIds[sname][id])
	delete(c.xreadIds[sname], id)
	if len(c.xreadIds[sname]) == 0 {
		delete(c.xreadIds, sname)
	}
	return nil
}

func NewClient(conn *net.Conn) *Client {
	return &Client{
		transaction:         utils.NewDeque[*resp.RESPData](),
		isInTransactionMode: false,

		isInSubscribeMode: false,
		PubSubChan:       make(chan string),
		publishers:        utils.NewSet[string](),

		conn:      conn,
		ClientInputChan: make(chan []byte),

		BlockOpChan: make(chan *resp.RESPData),
		blpopKeys:   utils.NewSet[string](),
		xreadIds:    make(map[string]map[string]chan *resp.RESPData),
	}
}
