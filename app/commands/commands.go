package commands

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

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

func checkArgCountEquals(cmd string, args []*resp.RESPData, expected int) error {
	if len(args) != expected {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command: want %d, but got %d", strings.ToLower(cmd), expected, len(args))
	}
	return nil
}

func checkArgCountGreaterThan(cmd string, args []*resp.RESPData, low int) error {
	if len(args) <= low {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command: want greater than %d, but got %d", strings.ToLower(cmd), low, len(args))
	}
	return nil
}

func checkArgCountBetween(cmd string, args []*resp.RESPData, low int, high int) error {
	if len(args) < low || len(args) > high {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command: want %d-%d, but got %d", strings.ToLower(cmd), low, high, len(args))
	}
	return nil
}

func checkArgCountSatisfies(cmd string, args []*resp.RESPData, cond func(int) bool, errorMsg string) error {
	if !cond(len(args)) {
		return fmt.Errorf("ERR wrong number of arguments for '%s': number of arguments %d does not satisfy condition: %s", strings.ToLower(cmd), len(args), errorMsg)
	}
	return nil
}

func (h *CommandHandler) HandleCOMMANDDOCS(args []*resp.RESPData) (*resp.RESPData, error) {
	// redis-cli sends COMMAND DOCS when a client connects
	// Return empty array for now

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

func (h *CommandHandler) HandleECHO(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("echo", args, 2); err != nil {
		return nil, err
	}
	return args[1], nil
}

func (h *CommandHandler) HandlePING(args []*resp.RESPData, inSubscribeMode bool) (*resp.RESPData, error) {
	if err := checkArgCountEquals("ping", args, 1); err != nil {
		return nil, err
	}

	if !inSubscribeMode {
		return resp.ConvertSimpleStringToRESP("PONG"), nil
	}

	return resp.ConvertListToRESP([]string{"pong", ""}), nil

}

func (h *CommandHandler) HandleEXEC(args []*resp.RESPData, conn net.Conn) (*resp.RESPData, error) {
	if err := checkArgCountEquals("exec", args, 1); err != nil {
		return nil, err
	}

	// Check if connection already in the process of making transaction; if not, return error
	h.db.Lock()
	commands, ok := h.db.GetTransaction(conn)
	if !ok {
		h.db.DeleteTransaction(conn)
		h.db.Unlock()
		return nil, fmt.Errorf("ERR EXEC without MULTI")
	}

	// Delete the transaction
	h.db.DeleteTransaction(conn)
	h.db.Unlock()

	// Iterate through each command in the transaction
	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	for _, c := range commands {
		_, respCommand, suc := resp.DecodeFromRESP(c)
		// If unable to decode RESP byte array, capture error
		if !suc {
			ret.ListRESPData = append(ret.ListRESPData, resp.ConvertSimpleErrorToRESP("ERR unable to decode command"))

			// Capture error from handling command or append the output of handling the command.
		} else if res, err := h.Handle(respCommand, conn, false); err != nil {
			ret.ListRESPData = append(ret.ListRESPData, resp.ConvertSimpleErrorToRESP(err.Error()))
		} else {
			ret.ListRESPData = append(ret.ListRESPData, res)
		}

	}

	return ret, nil
}

func (h *CommandHandler) HandleMULTI(args []*resp.RESPData, conn net.Conn) (*resp.RESPData, error) {
	if err := checkArgCountEquals("multi", args, 1); err != nil {
		return nil, err
	}
	// Create new transaction if nonexistent
	h.db.Lock()
	defer h.db.Unlock()
	if _, ok := h.db.GetTransaction(conn); !ok {
		h.db.CreateTransaction(conn)
		return resp.ConvertSimpleStringToRESP("OK"), nil
	}

	// Cannot call MULTI while already in a transaction
	return nil, fmt.Errorf("ERR cannot call MULTI while already in a transaction")

}

func (h *CommandHandler) HandleDISCARD(args []*resp.RESPData, conn net.Conn) (*resp.RESPData, error) {
	if err := checkArgCountEquals("discard", args, 1); err != nil {
		return nil, err
	}
	h.db.Lock()
	defer h.db.Unlock()

	// Error if not in transaction
	if _, ok := h.db.GetTransaction(conn); !ok {
		return nil, fmt.Errorf("ERR DISCARD without MULTI")
	}

	// Otherwise, simply delete the transaction
	h.db.DeleteTransaction(conn)
	return resp.ConvertSimpleStringToRESP("OK"), nil
}

func (h *CommandHandler) HandleSUBSCRIBE(args []*resp.RESPData, conn net.Conn) (*resp.RESPData, error) {
	if err := checkArgCountEquals("subscribe", args, 2); err != nil {
		return nil, err
	}

	pubChanName := args[1].String()

	h.db.Lock()

	// Add channel to publisher client list
	h.db.AddSubscriber(pubChanName, conn)

	// Create a channel (if doesn't exist yet) for this client to receive values
	h.db.CreateReceiver(conn)

	// Add publisher channel name to list of subscribed channels for this client
	h.db.AddPublisher(conn, pubChanName)

	// Save number of subscriptions for output
	pubs, _ := h.db.GetPublishers(conn)
	numSubscribed := pubs.Length()

	h.db.Unlock()

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	ret.ListRESPData = append(ret.ListRESPData,
		resp.ConvertBulkStringToRESP("subscribe"),
		resp.ConvertBulkStringToRESP(pubChanName),
		resp.ConvertIntToRESP(int64(numSubscribed)))
	return ret, nil
}

func (h *CommandHandler) HandleUNSUBSCRIBE(args []*resp.RESPData, conn net.Conn) (*resp.RESPData, error) {
	if err := checkArgCountEquals("unsubscribe", args, 2); err != nil {
		return nil, err
	}

	pubChanName := args[1].String()
	remSubscribed := 0

	h.db.Lock()
	h.db.RemovePublisherFromSubscriber(conn, pubChanName)
	h.db.RemoveSubscriberFromPublisher(pubChanName, conn)
	if pubs, ok := h.db.GetPublishers(conn); ok {
		remSubscribed = pubs.Length()
	}

	h.db.Unlock()

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}
	ret.ListRESPData = append(ret.ListRESPData,
		resp.ConvertBulkStringToRESP("unsubscribe"),
		resp.ConvertBulkStringToRESP(pubChanName),
		resp.ConvertIntToRESP(int64(remSubscribed)),
	)

	return ret, nil

}

func (h *CommandHandler) HandlePUBLISH(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("publish", args, 3); err != nil {
		return nil, err
	}

	channelName := args[1].String()
	messageContents := args[2].String()

	numSubscribers := 0
	h.db.Lock()
	if subs, ok := h.db.GetSubscribers(channelName); ok {
		numSubscribers = subs.Length()
		for conn := range subs.Items() {
			go func() {
				receiver, ok := h.db.GetReceiver(conn)
				// Make sure client still exists
				if !ok {
					return
				}
				select {
				case receiver <- messageContents:
				default: // skip slow subscribers
				}

			}()
		}
	}
	h.db.Unlock()

	return resp.ConvertIntToRESP(int64(numSubscribers)), nil

}

func (h *CommandHandler) HandleSET(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountSatisfies("set", args, func(len int) bool {return len == 3 || len == 5}, "Number of arguments should be either 3 or 5"); err != nil {
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

func (h *CommandHandler) HandleGET(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("get", args, 2); err != nil {
		return nil, err
	}
	key := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()
	val, ok := h.db.GetString(key)
	if !ok {
		return &resp.RESPData{Type: resp.BulkString}, nil
	}
	return resp.ConvertBulkStringToRESP(val), nil
}

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
	} else if intVal, err := strconv.ParseInt(val, 10, 64); err != nil {
		return nil, fmt.Errorf("ERR %v", err)

		// Otherwise we can increment the key
	} else {
		newVal = intVal + 1
	}

	h.db.SetString(key, strconv.FormatInt(newVal, 10))
	return resp.ConvertIntToRESP(newVal), nil
}

func (h *CommandHandler) HandleRPUSH(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountGreaterThan("rpush", args, 2); err != nil {
		return nil, err
	}

	// The name of the array for which we want to push
	key := args[1].String()

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

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if waitChans, ok := h.db.GetBLPOPWaiters(key); ok && len(waitChans) > 0 {
		popped := listData[0]
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0])    // Close the channel

		h.db.PopBLPOPWaiter(key) // Remove client off the queue
	}

	// Return length of array
	return resp.ConvertIntToRESP(int64(len(listData))), nil

}

func (h *CommandHandler) HandleLPUSH(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountGreaterThan("lpush", args, 2); err != nil {
		return nil, err
	}

	// The name of the array for which we want to push
	key := args[1].String()

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

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if waitChans, ok := h.db.GetBLPOPWaiters(key); ok && len(waitChans) > 0 {
		popped := listData[0]
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0])    // Close the channel

		h.db.PopBLPOPWaiter(key) // Remove client off the queue
	}

	return resp.ConvertIntToRESP(int64(len(listData))), nil

}

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
	// Parse start and stop indices, handling negative indices and out-of-bounds cases
	start, errStart := strconv.Atoi(args[2].String())
	stop, errLow := strconv.Atoi(args[3].String())
	if errStart != nil {
		return nil, fmt.Errorf("ERR: %v", errStart)
	}
	if errLow != nil {
		return nil, fmt.Errorf("ERR: %v", errLow)
	}
	if start < 0 {
		start = arrLen + start
		start = max(0, start)
	}
	
	if stop < 0 {
		stop = arrLen + stop
		stop = max(0, stop)
	}
	stop = min(stop, arrLen-1)
	if start >= arrLen || start < 0 || stop < start || arrLen == 0 {
		return &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}, nil
	}

	return resp.ConvertListToRESP(val[start : stop+1]), nil

}

func (h *CommandHandler) HandleLLEN(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("llen", args, 2); err != nil {
		return nil, err
	}

	arrName := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()

	// If list doesn't exist, return 0
	arrResp, ok := h.db.GetList(arrName)
	if !ok {
		return resp.ConvertIntToRESP(0), nil
	}

	// Return length of list
	return resp.ConvertIntToRESP(int64(len(arrResp))), nil

}

func (h *CommandHandler) HandleBLPOP(args []*resp.RESPData) (*resp.RESPData, error) {
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

	h.db.Lock()

	data, ok := h.db.GetList(key)

	// If the array exists and has elements, pop and return immediately
	if ok && len(data) > 0 {
		poppedVal := data[0]
		h.db.SetList(key, data[1:])
		h.db.Unlock()
		return resp.ConvertListToRESP([]string{key, poppedVal}), nil
	}

	// Add client to list of blocked clients on this array
	c := h.db.AddBLPOPWaiter(key)
	h.db.Unlock() // Make sure to remove mutex so other clients can modify DB while this one is blocked

	// If duration is 0, block indefinitely until channel receives value
	if duration == 0.0 {
		poppedVal := <-c
		h.db.Lock()
		defer h.db.Unlock()
		h.db.PopLeftList(key) // Remove the popped element from the array
		return resp.ConvertListToRESP([]string{key, poppedVal}), nil
	}

	// Otherwise block until channel receives value or timeout occurs
	select {

	case <-time.After(time.Duration(duration * float64(time.Second))):
		// Timeout occurred, remove client from waiters list
		h.db.Lock()
		defer h.db.Unlock()
		// Remove channel from waiters list
		waitChans, _ := h.db.GetBLPOPWaiters(key)
		for i, chanVal := range waitChans {
			if chanVal == c {
				h.db.RemoveBLPOPWaiter(key, i)
				break
			}
		}
		return &resp.RESPData{Type: resp.Array, ListRESPData: nil}, nil

	case poppedVal := <-c:
		// Successfully received popped value
		h.db.Lock()
		defer h.db.Unlock()
		h.db.PopLeftList(key) // Remove the popped element from the array
		return resp.ConvertListToRESP([]string{key, poppedVal}), nil

	}

}

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

	// Return single element if only one was removed
	if numToRemove == 1 {
		return resp.ConvertBulkStringToRESP(ret[0]), nil
	}
	// Return array of removed elements otherwise
	return resp.ConvertListToRESP(ret), nil

}

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

func (h *CommandHandler) HandleXADD(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountSatisfies("xadd", args, func(len int) bool {return len >= 5 && len % 2 == 1}, "Number of arguments should be odd and at least 5"); err != nil {
		return nil, err
	}
	

	sname := args[1].String()

	h.db.Lock()
	defer h.db.Unlock()

	// Check if stream doesn't already exists
	stream, ok := h.db.GetStream(sname)

	// Make sure we can make a stream with the specified name
	if !ok && !h.db.CanSetStream(sname) {
		return nil, fmt.Errorf("ERR key '%s' with non-stream data type already exists", sname)
	} else if !ok {
		stream = make([]*storage.StreamEntry, 0)
	}

	id := args[2].String()

	// Validate ID

	// Cannot be 0-0
	if id == "0-0" {
		return nil, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	// Edge case: no previous entries and millis is 0
	} else if id == "0-*" && len(stream) == 0 {
		id = "0-1"
	// If ID is *, generate new ID based on previous entry (if exists)
	} else if id == "*" && len(stream) == 0 {
		id = fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0)
	} else if id == "*" {
		id = generateNewStreamID(stream[len(stream)-1].GetID())
	// Make sure ID is in the format int-int or int-*
	} else if idParts := strings.Split(id, "-"); len(idParts) != 2 {
		return nil, fmt.Errorf("ERR invalid ID format")
	} else if millis, err1 := strconv.Atoi(idParts[0]); err1 != nil {
		return nil, fmt.Errorf("ERR invalid ID format")
	} else if seqNum, err2 := strconv.Atoi(idParts[1]); err2 != nil && idParts[1] != "*" {
		return nil, fmt.Errorf("ERR invalid ID format")
	// Make sure millis is greater than or equal to previous entry's millis
	} else if len(stream) > 0 && millis < stream[len(stream)-1].GetMillis() {
		return nil, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	// Handle int-* case
	} else if idParts[1] == "*" && len(stream) > 0 && millis == stream[len(stream)-1].GetMillis() {
		id = fmt.Sprintf("%d-%d", millis, stream[len(stream)-1].GetSeqNum()+1)
	} else if idParts[1] == "*" {
		id = fmt.Sprintf("%d-0", millis)
	// Make sure seqNum is greater than previous entry's seqNum if millis are equal
	} else if len(stream) > 0 && seqNum <= stream[len(stream)-1].GetSeqNum() && millis == stream[len(stream)-1].GetMillis() {
		return nil, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	} else {
		// ID is valid, do nothing
	}

	// Populate the stream entry to be added to the stream
	entry := storage.NewStreamEntry(id)
	for i := 3; i < len(args); i += 2 {
		key := args[i].String()
		val := args[i+1].String()
		entry.Set(key, val)
	}

	// Update stream in DB
	stream = append(stream, entry)
	h.db.SetStream(sname, stream)

	// Find and wake up all relevant XREAD waiters

	// Those waiting for IDs greater than a specific ID
	idWaiters, ok := h.db.GetXREADIDWaiters(sname)
	if ok {
		for waiterId, chs := range idWaiters {
			if CompareStreamIDs(id, waiterId) == 1 {
				for _, ch := range chs {
					go func() {
						select {
						case ch <- entry:
						default:
						}
					}()
				}
			}
		}
	}

	// Those waiting for any new IDs
	allWaiters, ok := h.db.GetXREADAllWaiters(sname)
	if ok {
		for _, ch := range allWaiters {
			go func() {
				select {
				case ch <- entry:
				default:
				}
			}()
		}
	}

	// Return the ID of the stream that was just added
	return resp.ConvertBulkStringToRESP(id), nil
}

func generateNewStreamID(prevId string) string {
	prevIdParts := strings.Split(prevId, "-")
	prevMillis, _ := strconv.Atoi(prevIdParts[0])
	prevSeqNum, _ := strconv.Atoi(prevIdParts[1])

	currMillis := time.Now().UnixMilli()
	seqNum := 0
	if currMillis < int64(prevMillis) {
		currMillis = int64(prevMillis)
	}

	if currMillis == int64(prevMillis) {
		seqNum = prevSeqNum + 1
	}

	return fmt.Sprintf("%d-%d", currMillis, seqNum)
}

func (h *CommandHandler) HandleXRANGE(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountEquals("xrange", args, 4); err != nil {
		return nil, err
	}

	sname := args[1].String()
	id1 := args[2].String()
	id2 := args[3].String()

	// Validate IDs
	if id1 == "-" {
		id1 = "0-0"
	} else if id2 == "+" {
		id2 = fmt.Sprintf("%d-%d", math.MaxInt, math.MaxInt)
	} else {
		id1Parts := strings.Split(id1, "-")
		id2Parts := strings.Split(id2, "-")
		if len(id1Parts) > 2 || len(id2Parts) > 2 {
			return nil, fmt.Errorf("ERR invalid ID format")
		}

		millis1, err1 := strconv.Atoi(id1Parts[0])
		millis2, err2 := strconv.Atoi(id2Parts[0])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("ERR invalid ID format")
		}

		seqNum1, seqNum2 := 0, math.MaxInt
		if len(id1Parts) == 2 {
			seqNum1, err1 = strconv.Atoi(id1Parts[1])
		}
		if len(id2Parts) == 2 {
			seqNum2, err2 = strconv.Atoi(id2Parts[1])
		}

		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("ERR invalid ID format")
		}
		id1 = fmt.Sprintf("%d-%d", millis1, seqNum1)
		id2 = fmt.Sprintf("%d-%d", millis2, seqNum2)

	}

	ret := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}

	// If first ID is greater than second, return empty list right away
	if CompareStreamIDs(id1, id2) == 1 {
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
	for ; i < len(stream) && CompareStreamIDs(stream[i].GetID(), id1) == -1; i++ {

	}

	// Add elements that are in range
	for ; i < len(stream) && CompareStreamIDs(stream[i].GetID(), id2) != 1; i++ {
		// Append entry to return list
		ret.ListRESPData = append(ret.ListRESPData, stream[i].RESPData())
	}

	return ret, nil

}

func (h *CommandHandler) HandleXREAD(args []*resp.RESPData) (*resp.RESPData, error) {
	if err := checkArgCountSatisfies("xadd", args, func(len int) bool {return len >= 4 && len % 2 == 0}, "Number of arguments should be even and at least 4"); err != nil {
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
	snameToIdx := make(map[string]int)

	// Validate each ID
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

	type WaitChanResult struct {
		streamKey string
		results   []*storage.StreamEntry
	}

	results := make(chan *WaitChanResult, numStreams)

	for idx := firstStreamIndex; idx <= lastStreamIndex; idx++ {
		// Lock DB while checking if stream exists
		h.db.Lock()
		// For each stream, check if it exists
		sname := args[idx].String()
		stream, ok := h.db.GetStream(sname)
		if !ok {
			return nil, fmt.Errorf("ERR stream with key '%s' does not exist", sname)
		}
		id := args[idx+numStreams].String()

		go func() {
			res := &WaitChanResult{streamKey: sname, results: make([]*storage.StreamEntry, 0)}

			// If this isn't a blocking call, immediately send the relevant stream entries
			// If this is a blocking call and the stream isn't empty and contains relevant elements, return with the relevant stream entries
			if id != "$" && (!blocking || (blocking && len(stream) > 0)) {
				i := 0
				for ; i < len(stream) && CompareStreamIDs(stream[i].GetID(), id) != 1; i++ {

				}

				// Either this is a non-blocking call, or we found relevant entries right away
				if !blocking || i != len(stream) {
					res.results = append(res.results, stream[i:]...)
					results <- res
					return
				}
			}

			// Otherwise, create a channel to wait on which XADD will notify when new entries are added
			receiver := make(chan *storage.StreamEntry)

			// If id = "$", add to list of channels under stream key of xReadAllWaiters
			if id == "$" {
				h.db.AddXREADAllWaiter(sname, receiver)
				// Make sure to remove channel from waiters list once done
				defer func() {
					h.db.Lock()
					defer h.db.Unlock()
					waiters, _ := h.db.GetXREADAllWaiters(sname)
					for i, ch := range waiters {
						if ch == receiver {
							h.db.RemoveXREADAllWaiter(sname, i)
							break
						}
					}
				}()

				// Otherwise add to list of channels under id key under stream key of xReadIdWaiters
			} else {
				h.db.AddXREADIDWaiter(sname, id, receiver)

				// Make sure to remove channel from waiters list once done
				defer func() {
					h.db.Lock()
					defer h.db.Unlock()
					allIdWaiters, _ := h.db.GetXREADIDWaiters(sname)
					for i, ch := range allIdWaiters[id] {
						if ch == receiver {
							h.db.RemoveXREADIDWaiter(sname, id, i)
							break
						}
					}
				}()
			}

			// If duration = 0, block indefinitely
			if blockDurationMillis == 0 && blocking {
				entry := <-receiver
				res.results = append(res.results, entry)
				results <- res
				return
			}
			// Otherwise do a select statement, blocking until timeout is reached
			select {
			// If timeout is reached: return nil
			case <-time.After(time.Duration(blockDurationMillis * float64(time.Millisecond))):
				results <- res
				return

			// Otherwise: send the relevant stream entries
			case entry := <-receiver:
				res.results = append(res.results, entry)
				results <- res
				return
			}

		}()
		h.db.Unlock()
	}

	ret := &resp.RESPData{Type: resp.Array}
	for i := 0; i < numStreams; i++ {
		waitChanRes := <-results
		if len(waitChanRes.results) == 0 {
			continue
		}
		streamResults := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 2)}
		streamResults.ListRESPData[0] = resp.ConvertBulkStringToRESP(waitChanRes.streamKey)
		streamResultIds := &resp.RESPData{Type: resp.Array, ListRESPData: make([]*resp.RESPData, 0)}

		for _, res := range waitChanRes.results {
			streamResultIds.ListRESPData = append(streamResultIds.ListRESPData, res.RESPData())
		}
		streamResults.ListRESPData[1] = streamResultIds

		if ret.ListRESPData == nil {
			ret.ListRESPData = make([]*resp.RESPData, numStreams)
		}

		// Make sure to sort ret by stream names
		ret.ListRESPData[snameToIdx[waitChanRes.streamKey]] = streamResults
	}

	// If no streams had any relevant entries, return null array
	if len(ret.ListRESPData) == 0 {
		return &resp.RESPData{Type: resp.Array}, nil
	}

	return ret, nil
}

// CompareStreamIDs compares two valid stream IDs.
// Returns -1 if idA < idB, 1 if idA > idB, and 0 if they are equal.
func CompareStreamIDs(idA string, idB string) int {
	idAParts := strings.Split(idA, "-")
	idBParts := strings.Split(idB, "-")

	millisA, _ := strconv.Atoi(idAParts[0])
	millisB, _ := strconv.Atoi(idBParts[0])

	if millisA < millisB {
		return -1
	} else if millisA > millisB {
		return 1
	} else {
		seqNumA, _ := strconv.Atoi(idAParts[1])
		seqNumB, _ := strconv.Atoi(idBParts[1])
		if seqNumA < seqNumB {
			return -1
		} else if seqNumA > seqNumB {
			return 1
		} else {
			return 0
		}
	}
}

func (h *CommandHandler) Handle(respData *resp.RESPData, conn net.Conn, inTransaction bool) (*resp.RESPData, error) {

	request := respData.ListRESPData
	firstWord := strings.ToLower(string(request[0].Data))

	var res *resp.RESPData
	var err error

	// If the command is being done under a transaction, and isn't exec, multi, or discard, simply queue it, don't execute it.
	if inTransaction && firstWord != "exec" && firstWord != "multi" && firstWord != "discard" {
		h.db.Lock()
		if _, ok := h.db.GetTransaction(conn); ok {
			respRequest, _ := respData.EncodeToRESP()
			h.db.AddToTransaction(conn, respRequest)
			h.db.Unlock()
			return resp.ConvertSimpleStringToRESP("QUEUED"), nil
		}
		h.db.Unlock()
	}

	// Otherwise, proceed as normal and handle the message
	switch firstWord {

	// General
	case "command":
		res, err = h.HandleCOMMANDDOCS(request)
	case "echo":
		res, err = h.HandleECHO(request)
	case "ping":
		res, err = h.HandlePING(request, false)
	case "type":
		res, err = h.HandleTYPE(request)

	// Transactions
	case "exec":
		res, err = h.HandleEXEC(request, conn)
	case "multi":
		res, err = h.HandleMULTI(request, conn)
	case "discard":
		res, err = h.HandleDISCARD(request, conn)

	// Pub-sub
	case "subscribe":
		res, err = h.HandleSUBSCRIBE(request, conn)
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
		res, err = h.HandleBLPOP(request)

	// Stream
	case "xadd":
		res, err = h.HandleXADD(request)
	case "xrange":
		res, err = h.HandleXRANGE(request)
	case "xread":
		res, err = h.HandleXREAD(request)
	default:
		err = fmt.Errorf("ERR unknown command '%s'", firstWord)
	}

	return res, err
}

func (h *CommandHandler) HandleSubscribeMode(respData *resp.RESPData, conn net.Conn) (*resp.RESPData, error) {
	request := respData.ListRESPData
	firstWord := strings.ToLower(string(request[0].Data))

	var res *resp.RESPData
	var err error

	switch firstWord {
	case "subscribe":
		res, err = h.HandleSUBSCRIBE(request, conn)
	case "unsubscribe":
		res, err = h.HandleUNSUBSCRIBE(request, conn)
	case "psubscribe":
	case "punsubscribe":
	case "ping":
		res, err = h.HandlePING(request, true)
	case "quit":
	default:
		err = fmt.Errorf("ERR can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", firstWord)
	}

	return res, err
}
