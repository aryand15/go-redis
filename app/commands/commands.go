package commands

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"sync"
	"github.com/aryand15/go-redis/app/client"
	"github.com/aryand15/go-redis/app/storage"
	"github.com/aryand15/go-redis/resp"
)

type CommandHandler struct {
	db *storage.DB
}

func NewCommandHandler(db *storage.DB) *CommandHandler {
	return &CommandHandler{db: db}
}

func (h *CommandHandler) GetDB() *storage.DB {
	return h.db
}

// checkArgCountEquals checks if the number of arguments in the given command is equal to the given expected value, 
// returning an error if that is not the case.
func checkArgCountEquals(cmd string, args []*resp.RESPData, expected int) error {
	if len(args) != expected {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command: want %d, but got %d", strings.ToLower(cmd), expected, len(args))
	}
	return nil
}

// checkArgCountGreaterThan checks if the number of arguments in the given command is greater than the threshold low, 
// returning an error if that is not the case.
func checkArgCountGreaterThan(cmd string, args []*resp.RESPData, low int) error {
	if len(args) <= low {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command: want greater than %d, but got %d", strings.ToLower(cmd), low, len(args))
	}
	return nil
}

// checkArgCountBetween checks if the number of arguments in the given command is in between the given low and high thresholds, inclusive, 
// returning an error if that is not the case.
func checkArgCountBetween(cmd string, args []*resp.RESPData, low int, high int) error {
	if len(args) < low || len(args) > high {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command: want %d-%d, but got %d", strings.ToLower(cmd), low, high, len(args))
	}
	return nil
}

// checkArgCountSatisfies checks if the number of arguments in the given command satisfies a given custom condition, 
// returning an error with an error message representing the issue for that custom condition if it is not satisfied.
func checkArgCountSatisfies(cmd string, args []*resp.RESPData, cond func(int) bool, errorMsg string) error {
	if !cond(len(args)) {
		return fmt.Errorf("ERR wrong number of arguments for '%s': number of arguments %d does not satisfy condition: %s", strings.ToLower(cmd), len(args), errorMsg)
	}
	return nil
}

// HandleCOMMANDDOCS handles the response after redis-cli sends "COMMAND DOCS" when a client connects by returning an empty array for now.
// It returns an error if the message contained in args starts with "COMMAND" but is not followed by "DOCS".
func (h *CommandHandler) HandleCOMMANDDOCS(args []*resp.RESPData) (*resp.RESPData, error) {

	if err := checkArgCountEquals("command", args, 2); err != nil {
		return nil, err
	}

	if strings.ToLower(args[1].String()) != "docs" {
		return nil, fmt.Errorf("ERR syntax error: expected 'COMMAND DOCS'")
	}

	return &resp.RESPData{
		Type:         resp.Array,
		ListRESPData: []*resp.RESPData{},
	}, nil
}

// HandleECHO handles a command in the form "ECHO message" by returning back the given message.
// It returns an error if the message contained in args does not have the approproate number of parts (2 in this case).
func (h *CommandHandler) HandleECHO(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("echo", args, 2); err != nil {
		return nil, err
	}
	return args[1], nil
}

// HandlePING handles the "PING" command.
// If in subscribe mode, it returns a RESP array with bulk string elements "pong" followed by "".
// Otherwise, it returns "PONG" as a bulk string.
// It returns an error if the message contained in args contains other arguments following "PING".
func (h *CommandHandler) HandlePING(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("ping", args, 1); err != nil {
		return nil, err
	}

	if !c.IsInSubscribeMode() {
		return resp.ConvertSimpleStringToRESP("PONG"), nil
	}

	return resp.ConvertListToRESP([]string{"pong", ""}), nil

}

// HandleQUIT handles the "QUIT" command, which allows the client to exit subscribe mode.
// It returns an error if the message contained in args contains other arguments following "QUIT".
// Otherwise, it returns "OK" as a simple string.
func (h *CommandHandler) HandleQUIT(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("quit", args, 1); err != nil {
		return nil, err
	}

	c.SetSubscribeMode(false)
	return resp.ConvertSimpleStringToRESP("OK"), nil

}

// HandleEXEC handles the "EXEC" command.
// It executes all commands in the client's transaction sequentially - where the client is represented by its unique network connection conn -
// and returns an array containing each command's output, which is either a successful message or an error, as a *RESPData instance.
//
// It returns an error if:
//   - the message contained in args contains other arguments following "EXEC".
//   - the client made the EXEC command without being in transaction mode (by executing MULTI).
func (h *CommandHandler) HandleEXEC(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("exec", args, 1); err != nil {
		return nil, err
	}

	// Check if connection already in the process of making transaction; if not, return error
	if !c.IsInTransactionMode() {
		return nil, fmt.Errorf("ERR EXEC without MULTI")
	}

	// Allocate structure to hold array of transaction command outputs
	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}

	// Iterate through each command in the transaction
	for command, err := c.PopFromTransaction(); err == nil; command, err = c.PopFromTransaction() {
		// Capture error from handling command or append the successful output of handling the command.
		if res, err := h.Handle(command, c); err != nil {
			ret.ListRESPData = append(ret.ListRESPData, resp.ConvertSimpleErrorToRESP(err.Error()))
		} else {
			ret.ListRESPData = append(ret.ListRESPData, res)
		}
	}

	c.EndTransaction()
	return ret, nil
}

// HandleMULTI handles the "MULTI" command.
// It starts a transaction for the client - where the client is represented by its unique network connection conn -
// if they are not already in one, and returns "OK" as a simple string on success.
//
// It returns an error if:
//   - the message contained in args contains other arguments following "MULTI".
//   - the client is already in the middle of a transaction.
func (h *CommandHandler) HandleMULTI(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("multi", args, 1); err != nil {
		return nil, err
	}

	// Cannot call MULTI while already in a transaction
	if c.IsInTransactionMode() {
		c.EndTransaction()
		return nil, fmt.Errorf("ERR cannot call MULTI while already in a transaction")
	}

	// Create new transaction if nonexistent
	c.StartTransaction()
	return resp.ConvertSimpleStringToRESP("OK"), nil
}

// HandleDISCARD handles the "DISCARD" command.
// If the client is in a transaction - where the client is represented by its unique network connection conn -
// it will discard the client's transaction and not execute any of its commands, bringing them back to the normal non-transactional mode,
// and will return "OK" as a simple string on success.
//
// It returns an error if:
//   - the message contained in args contains other arguments following "DISCARD".
//   - the client is not in the middle of a transaction.
func (h *CommandHandler) HandleDISCARD(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("discard", args, 1); err != nil {
		return nil, err
	}

	// Error if not in transaction
	if !c.IsInTransactionMode() {
		return nil, fmt.Errorf("ERR DISCARD without MULTI")
	}

	// Otherwise, simply delete the transaction
	c.EndTransaction()
	return resp.ConvertSimpleStringToRESP("OK"), nil
}

// HandleSUBSCRIBE handles the "SUBSCRIBE" command, where the command is in the format: "SUBSCRIBE channel".
//
// It subscribes the client - where the client is represented by its unique network connection conn -
// to the channel name specified in the second element of args, allowing it to immediately receive any values published to that channel
// from other clients. It returns an array with three elements: the word "subscribe", the name of the channel
// to subscribe to, and the updated number of channels the client is subscribed to.
// It returns an error if the message contained in args doesn't contain exactly two elements.
func (h *CommandHandler) HandleSUBSCRIBE(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("subscribe", args, 2); err != nil {
		return nil, err
	}

	pubChanName := args[1].String()
	if !c.IsInSubscribeMode() {
		c.SetSubscribeMode(true)
	}

	h.db.Lock()
	defer h.db.Unlock()
	h.db.AddSubscriber(c, pubChanName)
	clientPubs, _ := c.GetPublishers()
	numSubscribed := clientPubs.Length()
	
	

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	ret.ListRESPData = append(ret.ListRESPData,
		resp.ConvertBulkStringToRESP("subscribe"),
		resp.ConvertBulkStringToRESP(pubChanName),
		resp.ConvertIntToRESP(int64(numSubscribed)))
	return ret, nil
}

// HandleUNSUBSCRIBE handles the "UNSUBSCRIBE" command, where the command is in the format: "UNSUBSCRIBE channel".
//
// It unsubscribes the client - where the client is represented by its unique network connection conn -
// to the channel name specified in the second element of args, so that it no longer receives any values published to that channel
// from other clients. It returns an array with three elements: the word "unsubscribe", the name of the channel
// to subscribe to, and the updated number of channels the client is subscribed to.
// It returns an error if the message contained in args doesn't contain exactly two elements.
func (h *CommandHandler) HandleUNSUBSCRIBE(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("unsubscribe", args, 2); err != nil {
		return nil, err
	}

	pubChanName := args[1].String()
	h.db.Lock()
	defer h.db.Unlock()
	h.db.RemoveSubscriber(c, pubChanName)
	clientPubs, _ := c.GetPublishers()
	numSubscribed := clientPubs.Length()

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	ret.ListRESPData = append(ret.ListRESPData,
		resp.ConvertBulkStringToRESP("unsubscribe"),
		resp.ConvertBulkStringToRESP(pubChanName),
		resp.ConvertIntToRESP(int64(numSubscribed)),
	)

	return ret, nil

}

// HandlePUBLISH handles the "PUBLISH" command, where the command is in the format: "PUBLISH channel value".
//
// It publishes the specified value to the specified channel, immediately notifying all clients who are subscribed to that
// channel with the value.
// It returns the number of clients subscribed to that channel on success.
// It returns an error if the message contained in args doesn't contain exactly three elements.
func (h *CommandHandler) HandlePUBLISH(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("publish", args, 3); err != nil {
		return nil, err
	}

	channelName := args[1].String()
	messageContents := args[2].String()

	numSubscribers := 0
	h.db.Lock()
	defer h.db.Unlock()
	if subs, ok := h.db.GetSubscribers(channelName); ok {
		numSubscribers = subs.Length()
		for subscribedClient := range subs.Items() {
			go func(c *client.Client) {
				// Make sure client still exists; they might have disconnected
				if c.PubSubChan == nil {
					return
				}
				select {
				case c.PubSubChan <- messageContents:
				default: // skip clients that are slow
				}

			}(subscribedClient)
		}
	}

	return resp.ConvertIntToRESP(int64(numSubscribers)), nil

}

// HandleSET handles the "SET" command, where the command is in the format: "SET key value [EX seconds | PX milliseconds]".
//
// It sets the value of the specified key to the value specified by the user, with an optional specified expiration time
// (in seconds or milliseconds) after which the key will no longer exist. If the key doesn't exist, it will create the key and
// set it to the specified value.
//
// It returns "OK" as a simple string on success.
//
// It returns an error if:
//   - the message doesn't conform to the SET command structure specified above
//   - a stream, list, or any other non-string data type with the same key name already exists
//   - the expiration time is not an integer or is a negative integer
func (h *CommandHandler) HandleSET(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountSatisfies("set", args, func(len int) bool { return len == 3 || len == 5 }, "Number of arguments should be either 3 or 5"); err != nil {
		return nil, err
	}

	key := args[1].String()
	val := args[2].String()

	h.db.Lock()
	defer h.db.Unlock()

	// Make sure no other data type has the same key
	if !h.db.CanSetString(key) {
		return nil, fmt.Errorf("ERR key '%s' with non-string data type already exists", key)
	}

	if len(args) == 3 {
		h.db.SetString(key, val)
	} else if timeOption := args[3].String(); timeOption != "PX" && timeOption != "EX" {
		return nil, fmt.Errorf("ERR expecting PX or EX; got '%s' instead", timeOption)
	} else if duration, err := args[4].Int(); err != nil || duration < 0 {
		return nil, fmt.Errorf("ERR %v", err)
	} else if timeOption == "EX" {
		h.db.TimedSetString(key, val, time.Duration(duration)*time.Second)
	} else if timeOption == "PX" {
		h.db.TimedSetString(key, val, time.Duration(duration)*time.Millisecond)
	}
	return resp.ConvertSimpleStringToRESP("OK"), nil
}

// HandleGET handles the "GET" command, where the command is in the format: "GET key".
//
// It returns the value of the specified key if it exists, otherwise it returns the null bulk string.
// It returns an error if the message contained in args doesn't conform to the GET command structure specified above.
func (h *CommandHandler) HandleGET(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("get", args, 2); err != nil {
		return nil, err
	}
	key := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()
	val, ok := h.db.GetString(key)
	if !ok {
		return &resp.RESPData{Type: resp.BulkString}, nil // null bulk string
	}
	return resp.ConvertBulkStringToRESP(val), nil
}

// HandleINCR handles the "INCR" command, where the command is in the format: "INCR key".
//
// If the key exists and its value can be incremented to produce a signed, base-10 64-bit integer, the value gets incremented by one.
// If the key doesn't exist yet, a new key is created with value set to one.
// The method returns the new value of the key on success.
//
// It returns an error if:
//   - the message contained in args doesn't conform to the INCR command structure specified above
//   - the key doesn't exist yet but a stream, list, or any other non-string data type with the same key name already exists
//   - the key exists but cannot be represented as a signed, base-10 64-bit integer, or is the max possible signed, base-10 64-bit integer.
func (h *CommandHandler) HandleINCR(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("incr", args, 2); err != nil {
		return nil, err
	}

	key := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()

	val, ok := h.db.GetString(key)
	var newVal int64

	// If the key doesn't exist and is already in use, abort
	if !ok && !h.db.CanSetString(key) {
		return nil, fmt.Errorf("ERR key '%s' with non-string data type already exists", key)

		// Otherwise if the key doesn't exist and is available, set it to 1
	} else if !ok {
		newVal = 1

		// Otherwise if the key exists but can't be represented as a 64-bit integer, return an error
	} else if intVal, err := strconv.ParseInt(val, 10, 64); intVal == math.MaxInt64 || err != nil {
		return nil, fmt.Errorf("ERR key %s with value %s cannot be incremented and represented as a signed base-10 64-bit integer", key, val)

		// Otherwise we can increment the key
	} else {
		newVal = intVal + 1
	}

	h.db.SetString(key, strconv.FormatInt(newVal, 10))
	return resp.ConvertIntToRESP(newVal), nil
}

// HandleRPUSH handles the "RPUSH" command, where the command is in the format: "RPUSH key element [element ...]".
//
// It retrieves the list associated with the specified key and sequentially pushes the specified elements to the tail of the list.
// If the specified list key doesn't exist, it creates a fresh list with the specified elements in order.
// It returns the new length of the list on success as integer.
//
// A side effect of this function is that after it's done pushing elements, it notifies the first queued client
// who called BLPOP on this list (BLPOP is a blocking operation that forces a client to wait until an element is pushed to a desired list).
//
// It returns an error if:
//   - the message contained in args doesn't conform to the RPUSH command structure specified above
//   - the key doesn't exist yet but a stream, string, or any other non-list data type with the same key name already exists
func (h *CommandHandler) HandleRPUSH(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountGreaterThan("rpush", args, 2); err != nil {
		return nil, err
	}

	// The name of the array for which we want to push
	key := args[1].String()

	newList, err := func() ([]string, error) {
		h.db.Lock()
		defer h.db.Unlock()

		// Make sure no other data type has the same key
		if !h.db.CanSetList(key) {
			return nil, fmt.Errorf("ERR key '%s' with non-list data type already exists", key)
		}

		// If array doesn't exist, create an empty array belonging to that key
		if _, ok := h.db.GetList(key); !ok {
			h.db.SetList(key, make([]string, 0))
		}
		// Add all elements to the array
		for i := 2; i < len(args); i++ {
			elem := args[i].String()
			h.db.AppendToList(key, elem)
		}

		listData, _ := h.db.GetList(key)
		return listData, nil
	}()
	
	if err != nil {
		return nil, err
	}
	
	h.db.Lock()
	defer h.db.Unlock()

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if client, ok := h.db.GetNextBLPOPWaiter(key); ok {
		popped := args[2]
		client.BlockOpChan <- popped // Send the popped element through the channel to the client who called BLPOP first
		h.db.PopBLPOPWaiter(key, client) // Remove client off the queue
	}

	// Return length of array
	return resp.ConvertIntToRESP(int64(len(newList))), nil

}

// HandleLPUSH handles the "LPUSH" command, where the command is in the format: "LPUSH key element [element ...]".
//
// It retrieves the list associated with the specified key and sequentially pushes the specified elements to the head of the list.
// If the specified list key doesn't exist, it creates a fresh list with the specified elements added to the head of the list in order.
// It returns the new length of the list on success as integer.
//
// A side effect of this function is that after it's done pushing elements, it notifies the first queued client
// who called BLPOP on this list (BLPOP is a blocking operation that forces a client to wait until an element is pushed to a desired list).
//
// It returns an error if:
//   - the message contained in args doesn't conform to the LPUSH command structure specified above
//   - the key doesn't exist yet but a stream, string, or any other non-list data type with the same key name already exists
func (h *CommandHandler) HandleLPUSH(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountGreaterThan("lpush", args, 2); err != nil {
		return nil, err
	}

	// The name of the array for which we want to push
	key := args[1].String()

	newList, err := func() ([]string, error) {
		h.db.Lock()
		defer h.db.Unlock()
		
		// Make sure no other data type has the same key
		if !h.db.CanSetList(key) {
			return nil, fmt.Errorf("ERR key '%s' with non-list data type already exists", key)
		}

		// If array doesn't exist, create an empty array belonging to that key
		if _, ok := h.db.GetList(key); !ok {
			h.db.SetList(key, make([]string, 0))
		}

		// Prepend elements to array
		for i := 2; i < len(args); i++ {
			elem := args[i].String()
			h.db.PrependToList(key, elem)
		}
		listData, _ := h.db.GetList(key)
		return listData, nil
	}()

	if err != nil {
		return nil, err
	}

	h.db.Lock()
	defer h.db.Unlock()
	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if client, ok := h.db.GetNextBLPOPWaiter(key); ok {
		popped := args[len(args)-1]
		client.BlockOpChan <- popped // Send the popped element through the channel to the client who called BLPOP first
		h.db.PopBLPOPWaiter(key, client) // Remove client off the queue
	}
	
	return resp.ConvertIntToRESP(int64(len(newList))), nil

}

// HandleLRANGE handles the "LRANGE" command, where the command is in the format: "LRANGE key start stop".
//
// It returns a list containing elements associated with the specified list key whose indices fall between the start and stop indices, both inclusive.
// Negative indices indicate offset from the end of the list (e.g. -1 = last element).
// If the list key doesn't exist, or if the range is invalid, it returns an empty array.
// It returns an error if:
//   - the message contained in args doesn't conform to the LRANGE command structure specified above
//   - start and stop aren't valid signed base 10 64-bit integers
func (h *CommandHandler) HandleLRANGE(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("lrange", args, 4); err != nil {
		return nil, err
	}

	h.db.Lock()
	defer h.db.Unlock()

	// If list doesn't exist, return empty array
	val, ok := h.db.GetList(string(args[1].Data))
	if !ok {
		return &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}, nil
	}

	arrLen := len(val)

	// Start and/or stop could be invalid integer
	start, errStart := strconv.Atoi(args[2].String())
	stop, errLow := strconv.Atoi(args[3].String())

	if errStart != nil {
		return nil, fmt.Errorf("ERR: %v", errStart)
	}
	if errLow != nil {
		return nil, fmt.Errorf("ERR: %v", errLow)
	}

	// Handle negative indices
	if start < 0 {
		start = arrLen + start
	}
	if stop < 0 {
		stop = arrLen + stop
	}

	// Invalid [start, stop] ranges: both negative, both past array length, or start > stop
	if start < 0 && stop < 0 || start >= arrLen && stop >= arrLen || start > stop {
		return &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}, nil
	}

	// Set negative start index to 0, set overly large stop index to index of last element
	start = max(start, 0)
	stop = min(stop, arrLen-1)

	// Return list with elements between start and stop indices, inclusive
	return resp.ConvertListToRESP(val[start : stop+1]), nil

}

// HandleLLEN handles the "LLEN" command, where the command is in the format: "LLEN key".
//
// It returns the length of the list associated with the specified list key, or 0 if the list key doesn't exist.
// It returns an error if the message contained in args doesn't conform to the LLEN command structure specified above.
func (h *CommandHandler) HandleLLEN(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("llen", args, 2); err != nil {
		return nil, err
	}

	arrName := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()

	// If list doesn't exist, return 0
	arr, ok := h.db.GetList(arrName)
	if !ok {
		return resp.ConvertIntToRESP(0), nil
	}

	// Return length of list
	return resp.ConvertIntToRESP(int64(len(arr))), nil

}

// HandleBLPOP handles the "BLPOP" command, where the command is in the format: "BLPOP key timeout".
//
// It blocks the client from doing anything until another client pushes an element to the specified list key via LPUSH or RPUSH,
// where the timeout is a double float value indicating the max amount of time to block. If timeout value is set to 0,
// the client is blocked indefinitely. If the timeout expires, it returns a null bulk array. Otherwise, if the list at the specified key
// gets or has a value while the timeout has not expired, and the client is the first in the queue of all clients who performed
// BLPOP on this key, then the element at the head of the list is popped, and the function returns
// a two-element array with the name of the key followed by the popped element.
// If the list key doesn't exist, or if the range is invalid, it returns an empty array.
// It returns an error if:
//   - the message contained in args doesn't conform to the BLPOP command structure specified above
//   - timeout isn't a non-negative double float value.
func (h *CommandHandler) HandleBLPOP(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	if err := checkArgCountEquals("blpop", args, 3); err != nil {
		return nil, err
	}

	// Get the name of the array to pop from
	key := args[1].String()

	// Make sure duration is a non-negative double float
	duration, err := strconv.ParseFloat(string(args[2].Data), 64)
	if err != nil {
		return nil, fmt.Errorf("ERR %v", err)
	}
	if duration < 0 {
		return nil, fmt.Errorf("ERR duration cannot be negative")
	}

	popped, foundElemImmediately := func()(string, bool) {
		h.db.Lock()
		defer h.db.Unlock()
		data, ok := h.db.GetList(key)


		// If the array exists and has elements, pop and return immediately
		// No need to add client to queue of BLPOP waiters if this is the case
		if ok && len(data) > 0 {
			popped, _ := h.db.PopLeftList(key)
			return popped, true
		}

		// Add client to list of blocked clients on this array
		h.db.AddBLPOPWaiter(key, c)
		return "", false
	}()

	if foundElemImmediately {
		return resp.ConvertListToRESP([]string{key, popped}), nil
	}

	// If duration is 0, block indefinitely until channel receives value
	if duration == 0.0 {
		poppedVal, ok := <-c.BlockOpChan
		if !ok {	// check to see if client's channel has become nil or is closed on disconnect
			return nil, fmt.Errorf("client disconnected")
		}
		h.db.Lock()
		defer h.db.Unlock()
		h.db.PopLeftList(key) // Remove the popped element from the array
		return &resp.RESPData{
			Type: resp.Array,
			ListRESPData: []*resp.RESPData{resp.ConvertBulkStringToRESP(key), poppedVal},
		}, nil
	}

	// Otherwise block until channel receives value or timeout occurs
	select {

	case <-time.After(time.Duration(duration * float64(time.Second))):
		// Timeout occurred, remove client from waiters list
		h.db.Lock()
		defer h.db.Unlock()
		h.db.RemoveBLPOPWaiter(key, c)
		return &resp.RESPData{Type: resp.Array, ListRESPData: nil}, nil

	case poppedVal, ok := <-c.BlockOpChan:
		if !ok {	// check to see if client's channel has become nil or is closed on disconnect
			return nil, fmt.Errorf("client disconnected")
		}
		// Successfully received popped value
		h.db.Lock()
		defer h.db.Unlock()
		h.db.PopLeftList(key) // Remove the popped element from the array
		return &resp.RESPData{
			Type: resp.Array,
			ListRESPData: []*resp.RESPData{resp.ConvertBulkStringToRESP(key), poppedVal},
		}, nil

	}

}

// HandleLPOP handles the "LPOP" command, where the command is in the format: "LPOP key [count]".
//
// It removes a number of elements specified by the optional count argument (defaults to 1) from the head of the specified list key.
// It returns either a bulk string containing the single popped element if the count argument is omitted, or an array containing the
// one or more popped elements, in order of when they were popped, if the count argument is included.
// If the array with the specified list key doesn't exist, it returns a null bulk string
// It returns an error if:
//   - the message contained in args doesn't conform to the LPOP command structure specified above
//   - count isn't a non-negative signed 64-bit base-10 integer
func (h *CommandHandler) HandleLPOP(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountBetween("lpop", args, 2, 3); err != nil {
		return nil, err
	}
	arrName := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()

	// Return null string if array doesn't exist
	arrResp, ok := h.db.GetList(arrName)
	if !ok {
		return &resp.RESPData{Type: resp.BulkString}, nil
	}

	// Parse optional number of elements to remove
	numToRemove := 1
	if len(args) == 3 {
		parsedNum, err := strconv.Atoi(args[2].String())
		if err != nil {
			return nil, fmt.Errorf("ERR %v", err)
		}
		if parsedNum < 0 {
			return nil, fmt.Errorf("ERR number of elements to remove cannot be negative")
		}
		numToRemove = min(parsedNum, len(arrResp))
	}

	// Populate list of popped elements
	ret := make([]string, 0)
	ret = append(ret, arrResp[:numToRemove]...)

	// Update the array in the DB after popping elements
	if numToRemove == len(arrResp) {
		h.db.SetList(arrName, make([]string, 0))
	} else {
		h.db.SetList(arrName, arrResp[numToRemove:])
	}

	// Return single element (as bulk string) if the count argument was omitted
	if numToRemove == 1 && len(args) == 2 {
		return resp.ConvertBulkStringToRESP(ret[0]), nil
	}
	// Return array of removed elements if count argument was included otherwise (even if it was 1)
	return resp.ConvertListToRESP(ret), nil

}

// HandleTYPE handles the "TYPE" command, where the command is in the format: "TYPE key".
//
// As the name suggests, it returns the type (string, list, stream, etc.) of the specified key in simple string format,
// where the type is supported in this implementation.
// If the key doesn't exist, it returns "none" as a simple string.
// It returns an error if the message contained in args doesn't conform to the TYPE command structure specified above.
func (h *CommandHandler) HandleTYPE(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("type", args, 2); err != nil {
		return nil, err
	}

	h.db.Lock()
	defer h.db.Unlock()
	_, isString := h.db.GetString(args[1].String())
	if isString {
		return &resp.RESPData{Type: resp.SimpleString, Data: []byte("string")}, nil
	}
	_, isList := h.db.GetList(args[1].String())
	if isList {
		return &resp.RESPData{Type: resp.SimpleString, Data: []byte("list")}, nil
	}

	_, isStream := h.db.GetStream(args[1].String())
	if isStream {
		return &resp.RESPData{Type: resp.SimpleString, Data: []byte("stream")}, nil
	}

	return &resp.RESPData{Type: resp.SimpleString, Data: []byte("none")}, nil
}

// HandleXADD handles the "XADD" command, where the command is in the format: "XADD stream_key <* | id> field value [field value ...]".
//
// It appends a new stream entry with the given ID containing the specified field-value pairs to the stream associated with the specified stream key.
// If the stream key doesn't exist, it creates a new stream with the specified entry.
// It returns the ID of the newly added stream entry as a bulk string on success.
//
// A side effect of this function is that after adding the entry, it notifies all clients who are blocked on XREAD calls
// waiting for new entries on this stream.
//
// It returns an error if:
//   - the message contained in args doesn't conform to the XADD command structure specified above
//   - the key doesn't exist yet but a string, list, or any other non-stream data type with the same key name already exists
//   - the specified entry ID is invalid or less than or equal to the last entry ID in the stream
func (h *CommandHandler) HandleXADD(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountSatisfies("xadd", args, func(len int) bool { return len >= 5 && len%2 == 1 }, "Number of arguments should be odd and at least 5"); err != nil {
		return nil, err
	}

	// Handle stream key name
	sname := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()

	// Check if stream doesn't already exists
	stream, ok := h.db.GetStream(sname)

	// Make sure we can make a stream with the specified name
	if !ok && !h.db.CanSetStream(sname) {
		return nil, fmt.Errorf("ERR key '%s' with non-stream data type already exists", sname)
	} else if !ok {
		stream = storage.NewStream()
	}

	// Handle stream ID
	id := args[2].String()

	// Validate ID provided by user
	id, err := stream.CleanedNextStreamID(id)
	if err != nil {
		return nil, err
	}

	// Populate the stream entry to be added to the stream (handle key-value pairs)
	entry := storage.NewStreamEntry(id)
	for i := 3; i < len(args); i += 2 {
		key := args[i].String()
		val := args[i+1].String()
		entry.Set(key, val)
	}

	// Update stream in DB
	stream.AddEntry(entry)
	h.db.SetStream(sname, stream)

	// Find and wake up all relevant XREAD waiters
	idWaiters, ok := h.db.GetXREADIDWaiters(sname)
	if ok {
		for waiterId, clients := range idWaiters {
			if storage.CompareStreamIDs(id, waiterId) == 1 {
				for c := range clients.Items() {
					go func(client *client.Client) {
						entries := []*storage.StreamEntry{entry}
						xreadResult := singleXREADresult(sname, entries)
						clientXreadChans, _ := c.GetXREADIds()
						select {
						case clientXreadChans[sname][waiterId] <- xreadResult:
						default:	// account for case that client has dropped out
						}
					}(c)
				}
			}
		}
	}

	// Return the ID of the stream that was just added
	return resp.ConvertBulkStringToRESP(id), nil
}

// decipherXRANGEid is a helper function that is used to validate and format an ID provided by the client in an XRANGE command.
// Aside from the main id formatting of "int-int", the special ids "+", "-", or "int" are also valid values in XRANGE, demonstrating
// the need to handle these cases specially. The function takes the id along with a boolean isStartID indicating whether this is the start
// or end ID in the XRANGE command, based on which it might format the id differently. It returns the formatted id in "int-int" format on success,
// or an error if the provided id was somehow invalid to begin with.
func decipherXRANGEid(id string, isStartID bool) (string, error) {
	// Handle - and + IDs
	if id == "-" {
		return "0-0", nil
	}
	if id == "+" {
		return fmt.Sprintf("%d-%d", math.MaxInt64, math.MaxInt64), nil
	}

	// Make sure id looks like either int-int or int
	idParts := strings.Split(id, "-")
	if !(len(idParts) == 2 || len(idParts) == 1) {
		return "", fmt.Errorf("ERR invalid ID format") // wrong number of parts
	}
	millis, err := strconv.ParseInt(idParts[0], 10, 64)
	if err != nil { // first part of ID is not a signed base-10 64 bit integer
		return "", fmt.Errorf("ERR invalid ID format")
	}

	// Handle case when id just looks like "int"
	if len(idParts) == 1 && isStartID {
		return fmt.Sprintf("%d-0", millis), nil
	} else if len(idParts) == 1 && !isStartID {
		return fmt.Sprintf("%d-%d", millis, math.MaxInt64), nil
	}

	// int-int case
	seqNum, err := strconv.ParseInt(idParts[1], 10, 64)
	if err != nil { // second part of ID is not a signed base-10 64 bit integer
		return "", fmt.Errorf("ERR invalid ID format")
	}

	return fmt.Sprintf("%d-%d", millis, seqNum), nil
}

// HandleXRANGE handles the "XRANGE" command, where the command is in the format: "XRANGE key start end".
//
// It returns an array containing elements from the stream with the specified key whose IDs fall between the start and stop IDs,
// both inclusive. Special IDs include "+" which indicates the highest possible ID in the stream and "-" which indicates the lowest.
// If the range is invalid, it returns an empty array.
//
// It returns an error if:
//   - the message contained in args doesn't conform to the XRANGE command structure specified above
//   - the stream with the specified key does not exist
//   - the IDs provided are invalid to begin with
func (h *CommandHandler) HandleXRANGE(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("xrange", args, 4); err != nil {
		return nil, err
	}

	sname := args[1].String()

	// Validate IDs
	id1, err1 := decipherXRANGEid(args[2].String(), true)
	id2, err2 := decipherXRANGEid(args[3].String(), false)
	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
	}

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}

	// If first ID is greater than second, return empty list right away
	if storage.CompareStreamIDs(id1, id2) == 1 {
		return ret, nil
	}

	h.db.Lock()
	defer h.db.Unlock()

	// Check if stream exists
	stream, ok := h.db.GetStream(sname)
	if !ok {
		return nil, fmt.Errorf("ERR stream with key '%s' does not exist", sname)
	}

	i := 0
	// Find first stream element in range
	for ; i < stream.Length() && storage.CompareStreamIDs(stream.EntryAt(i).GetID(), id1) == -1; i++ {

	}

	// Add elements that are in range
	for ; i < stream.Length() && storage.CompareStreamIDs(stream.EntryAt(i).GetID(), id2) != 1; i++ {
		// Append entry to return list
		ret.ListRESPData = append(ret.ListRESPData, stream.EntryAt(i).RESPData())
	}

	return ret, nil

}

// HandleXREAD handles the "XREAD" command, where the command is in the format:
// "XREAD [BLOCK milliseconds] STREAMS key1 [key2 ...] id1 [id2 ...]".
//
// If the BLOCK option is omitted, it will immediately return an array containing the IDs and key-value pairs
// of all stream entries for each given stream key whose IDs are greater than the corresponding given stream ID.
//
// If the BLOCK option is included, the client will be blocked for the specified number of milliseconds until a relevant stream entry
// (with ID greater than the specified ID) gets added to one of the specified streams by another client. When in blocking mode,
// the special ID value "$" can be used to signify that the client wants only the newest values, and "0" indicates indefinite blocking.
//
// If no relevant stream entries are found in either mode, it returns the null array.
//
// It returns an error if:
//   - the message contained in args doesn't conform to the XREAD command structure specified above
//   - any of the provided IDs are invalid
func (h *CommandHandler) HandleXREAD(args []*resp.RESPData, c *client.Client) (*resp.RESPData, error) {
	// Check if command is structured correctly
	if err := checkArgCountSatisfies("xadd", args, func(len int) bool { return len >= 4 && len%2 == 0 }, "Number of arguments should be even and at least 4"); err != nil {
		return nil, err
	}
	if strings.ToLower(args[1].String()) != "streams" && strings.ToLower(args[1].String()) != "block" {
		return nil, fmt.Errorf("ERR expected STREAMS or BLOCK, got: %s", strings.ToLower(args[1].String()))
	} else if strings.ToLower(args[1].String()) == "block" && strings.ToLower(args[3].String()) != "streams" {
		return nil, fmt.Errorf("ERR expected STREAMS, got: %s", strings.ToLower(args[3].String()))
	}

	blocking := false
	blockDurationMillis := 0.0
	if strings.ToLower(args[1].String()) == "block" {
		blocking = true
		converted, err := strconv.ParseFloat(args[2].String(), 64)
		if err != nil {
			return nil, fmt.Errorf("ERR %v", err)
		}
		if converted < 0 {
			return nil, fmt.Errorf("ERR duration cannot be negative")
		}
		blockDurationMillis = converted
	}

	firstStreamIndex := 2
	if blocking {
		firstStreamIndex = 4
	}
	lastIdIndex := len(args) - 1
	numStreams := (lastIdIndex - firstStreamIndex + 1) / 2
	lastStreamIndex := firstStreamIndex + numStreams - 1
	firstIdIndex := lastStreamIndex + 1

	// Create map of stream names to order for sorting purposes later on
	// xread returns results in the order that the client inputted the keys, 
	// so we need to handle the case that results come in a different order
	snameToIdx := make(map[string]int)

	// Validate all IDs first
	for i := firstIdIndex; i < lastIdIndex+1; i++ {
		id := args[i].String()

		snameToIdx[args[i-numStreams].String()] = i - firstIdIndex

		// ID can be $ as long as this is a blocking call
		if id == "$" && blocking {
			continue
		} else if id == "$" {
			return nil, fmt.Errorf("ERR stream ID cannot be $ in a non-blocking call")
		}

		// Otherwise make sure it follows the format int-int
		idParts := strings.Split(id, "-")
		if len(idParts) != 2 {
			return nil, fmt.Errorf("ERR invalid ID format")
		}
		if _, err := strconv.Atoi(idParts[0]); err != nil {
			return nil, fmt.Errorf("ERR invalid ID format")
		}
		if _, err := strconv.Atoi(idParts[1]); err != nil {
			return nil, fmt.Errorf("ERR invalid ID format")
		}
	}

	// WaitChanResult holds the specific stream entries whose IDs are greater than the client-provided ID for the specific stream key.
	//type WaitChanResult struct {
	//	streamKey string
	//	result	
	//}

	// Make a channel that has sufficient capacity to hold results from all streams.
	//results := make(chan *resp.RESPData, numStreams)
	results := make([]*resp.RESPData, numStreams)
	var wg sync.WaitGroup

	for idx := firstStreamIndex; idx <= lastStreamIndex; idx++ {
		// Lock DB while checking if stream exists
		h.db.Lock()
		// For each stream, check if it exists
		sname := args[idx].String()
		stream, ok := h.db.GetStream(sname)
		
		// if nonblocking and no such stream exists, just skip
		if !ok && !blocking {
			h.db.Unlock()
			continue
		}
		// otherwise if blocking, and stream doesn't exist, we still have to wait
		if !ok {
			stream = storage.NewStream()
		}
		id := args[idx+numStreams].String()
		
		wg.Add(1)
		go func(streamName string, streamId string, stream *storage.Stream) {
			defer wg.Done()
			
			// If this isn't a blocking call, immediately send the relevant stream entries
			// If this is a blocking call and the stream isn't empty and contains relevant elements, return with the relevant stream entries
			if streamId != "$" && (!blocking || (blocking && stream.Length() > 0)) {
				i := 0
				for ; i < stream.Length() && storage.CompareStreamIDs(stream.EntryAt(i).GetID(), streamId) != 1; i++ {

				}

				// Either this is a non-blocking call, or we found relevant entries right away
				if !blocking || i != stream.Length() {
					slice, _ := stream.Slice(i)
					xreadResult := singleXREADresult(streamName, slice)
					results[idx-firstStreamIndex] = xreadResult
					return
				}
			}

			// Convert id = "$" to an actual ID
			if streamId == "$" {
				streamId = "0-0"
				if stream.Length() > 0 {
					streamId = stream.EntryAt(stream.Length()-1).GetID()
				}
			} 

			// Add to list of channels under id key under stream key of xReadIdWaiters
			h.db.AddXREADIDWaiter(streamName, streamId, c)

			// Make sure to remove channel from waiters list once done
			defer func() {
				h.db.Lock()
				defer h.db.Unlock()
				h.db.RemoveXREADIDWaiter(streamName, streamId, c)
			}()
			
			xreadChans, _ := c.GetXREADIds()
			// If duration = 0, block indefinitely
			if blockDurationMillis == 0 && blocking {
				xreadResult := <-xreadChans[streamName][streamId]
				results[idx-firstStreamIndex] = xreadResult
				return
			}
			// Otherwise do a select statement, blocking until timeout is reached
			select {
			// If timeout is reached: return nil
			case <-time.After(time.Duration(blockDurationMillis * float64(time.Millisecond))):
				results[idx-firstStreamIndex] = nil
				return

			// Otherwise: send the relevant stream entries
			case xreadResult := <-xreadChans[streamName][streamId]:
				results[idx-firstStreamIndex] = xreadResult
				return
			}

		}(sname, id, stream)
		h.db.Unlock()
	}

	wg.Wait()

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	for _, xreadResult := range results {
		if xreadResult == nil {
			continue
		}
		ret.ListRESPData = append(ret.ListRESPData, xreadResult)
	}

	// If no streams had any relevant entries, return null array
	if len(ret.ListRESPData) == 0 {
		return &resp.RESPData{Type: resp.Array}, nil
	}

	return ret, nil
}

func singleXREADresult(streamKey string, streamEntries []*storage.StreamEntry) *resp.RESPData {
	streamResults := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 2)}
	streamResults.ListRESPData[0] = resp.ConvertBulkStringToRESP(streamKey)
	streamResultIds := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}

	for _, res := range streamEntries {
		streamResultIds.ListRESPData = append(streamResultIds.ListRESPData, res.RESPData())
	}
	streamResults.ListRESPData[1] = streamResultIds
	return streamResults
}

// Handle takes in a RESP-serialized client command stored in respData,
// along with the client (represented by its unique TCP connection conn), and a boolean argument indicating whether
// the client is currently in transaction mode. It processes the command, using the client's connection details if needed,
// and returns back its response in a RESPData structure on success or an error if the command was not able to be successfully handled
// or if it is an unrecognized command.
func (h *CommandHandler) Handle(respData *resp.RESPData, client *client.Client) (*resp.RESPData, error) {
	if client.IsInSubscribeMode() {
		return h.handleSubscribeMode(respData, client)
	}

	request := respData.ListRESPData
	firstWord := strings.ToLower(string(request[0].Data))

	var res *resp.RESPData
	var err error

	// If the command is being done under a transaction, and isn't exec, multi, or discard, simply queue it, don't execute it.
	if client.IsInTransactionMode() && firstWord != "exec" && firstWord != "multi" && firstWord != "discard" {
		client.AddToTransaction(respData)
		return resp.ConvertSimpleStringToRESP("QUEUED"), nil
	}

	// Otherwise, proceed as normal and handle the message
	switch firstWord {

	// General
	case "command":
		res, err = h.HandleCOMMANDDOCS(request)
	case "echo":
		res, err = h.HandleECHO(request)
	case "ping":
		res, err = h.HandlePING(request, client)
	case "type":
		res, err = h.HandleTYPE(request)

	// Transactions
	case "exec":
		res, err = h.HandleEXEC(request, client)
	case "multi":
		res, err = h.HandleMULTI(request, client)
	case "discard":
		res, err = h.HandleDISCARD(request, client)

	// Pub-sub
	case "subscribe":
		res, err = h.HandleSUBSCRIBE(request, client)
	case "publish":
		res, err = h.HandlePUBLISH(request)

	// Key-value
	case "set":
		res, err = h.HandleSET(request)
	case "get":
		res, err = h.HandleGET(request)
	case "incr":
		res, err = h.HandleINCR(request)

	// List
	case "rpush":
		res, err = h.HandleRPUSH(request)
	case "lpush":
		res, err = h.HandleLPUSH(request)
	case "lrange":
		res, err = h.HandleLRANGE(request)
	case "llen":
		res, err = h.HandleLLEN(request)
	case "lpop":
		res, err = h.HandleLPOP(request)
	case "blpop":
		res, err = h.HandleBLPOP(request, client)

	// Stream
	case "xadd":
		res, err = h.HandleXADD(request)
	case "xrange":
		res, err = h.HandleXRANGE(request)
	case "xread":
		res, err = h.HandleXREAD(request, client)
	default:
		err = fmt.Errorf("ERR unknown command '%s'", firstWord)
	}

	return res, err
}

// handleSubscribeMode is similar to Handle, but assumes the client is in subscribe mode, where they have access to a limited
// number of commands. It takes in a RESP-serialized client command stored in respData, along with the client state.
// (represented by its unique TCP connection conn), It processes the command, using the client's connection details if needed,
// and returns back its response in a RESPData structure on success or an error if the command was not able to be successfully handled
// or if it is an unsupported command in subscribe mode.
func (h *CommandHandler) handleSubscribeMode(respData *resp.RESPData, client *client.Client) (*resp.RESPData, error) {
	request := respData.ListRESPData
	firstWord := strings.ToLower(string(request[0].Data))

	var res *resp.RESPData
	var err error

	switch firstWord {
	case "subscribe":
		res, err = h.HandleSUBSCRIBE(request, client)
	case "unsubscribe":
		res, err = h.HandleUNSUBSCRIBE(request, client)
	case "psubscribe":
	case "punsubscribe":
	case "ping":
		res, err = h.HandlePING(request, client)
	case "quit":
		res, err = h.HandleQUIT(request, client)
	default:
		err = fmt.Errorf("ERR can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", firstWord)
	}

	return res, err
}
