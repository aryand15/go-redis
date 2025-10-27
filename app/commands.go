package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"math"
)

// Common RESP responses
var (
	respOK         = []byte("+OK\r\n")
	respPong       = []byte("+PONG\r\n")
	respNullString = []byte("$-1\r\n")
	respEmptyArr   = []byte("*0\r\n")
	respNullArr    = []byte("*-1\r\n")
	respNoneString = []byte("+none\r\n")
)

type CommandHandler struct {
	db *DB
}

func NewCommandHandler(db *DB) *CommandHandler {
	return &CommandHandler{db: db}
}

func (h *CommandHandler) HandleECHO(args []*RESPData) ([]byte, bool) {
	if len(args) < 2 {
		return nil, false
	}
	return EncodeToRESP(&RESPData{Type: BulkString, Data: args[1].Data})
}

func (h *CommandHandler) HandlePING() ([]byte, bool) {
	return respPong, true
}

func (h *CommandHandler) HandleSET(args []*RESPData) ([]byte, bool) {
	if len(args) != 3 && len(args) != 5 {
		return nil, false
	}
	key := args[1].String()
	val := args[2].String()

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// Make sure no other data type has the same key
	if !h.db.CanSetString(key) {
		return nil, false
	}

	if len(args) == 3 {
		h.db.SetString(key, val)
	} else if timeOption := args[3].String(); timeOption != "PX" && timeOption != "EX" {
		return nil, false
	} else if duration, err := args[4].Int(); err != nil || duration < 0 {
		return nil, false
	} else if timeOption == "EX" {
		h.db.TimedSetString(key, val, time.Duration(duration)*time.Second)
	} else if timeOption == "PX" {
		h.db.TimedSetString(key, val, time.Duration(duration)*time.Millisecond)
	}
	return respOK, true
}

func (h *CommandHandler) HandleGET(args []*RESPData) ([]byte, bool) {
	if len(args) < 2 {
		return nil, false
	}
	key := args[1].String()

	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	val, ok := h.db.GetString(key)
	if !ok {
		return respNullString, true
	}
	return EncodeToRESP(ConvertBulkStringToRESP(val))
}

func (h *CommandHandler) HandleRPUSH(args []*RESPData) ([]byte, bool) {
	if len(args) < 3 {
		return nil, false
	}

	// The name of the array for which we want to push
	key := args[1].String()

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// Make sure no other data type has the same key
	if !h.db.CanSetList(key) {
		return nil, false
	}

	// If array doesn't exist, create an empty array belonging to that key
	if _, ok := h.db.GetList(key); !ok {
		h.db.SetList(key, make([]string, 0))
	}

	// Add all elements to the array
	for i := 2; i < len(args); i++ {
		h.db.listData[key] = append(h.db.listData[key], args[i].String())
	}

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if waitChans, ok := h.db.blpopWaiters[key]; ok {
		popped := h.db.listData[key][0]
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0])    // Close the channel

		h.db.blpopWaiters[key] = h.db.blpopWaiters[key][1:] // Remove client off the queue
	}

	// Return length of array
	return EncodeToRESP(ConvertIntToRESP(len(h.db.listData[key])))

}

func (h *CommandHandler) HandleLPUSH(args []*RESPData) ([]byte, bool) {
	if len(args) < 3 {
		return nil, false
	}

	// The name of the array for which we want to push
	key := args[1].String()

	

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// Make sure no other data type has the same key
	if !h.db.CanSetList(key) {
		return nil, false
	}

	// If array doesn't exist, create an empty array belonging to that key
	if _, ok := h.db.GetList(key); !ok {
		h.db.SetList(key, make([]string, 0))
	}

	// Prepend elements to array
	for i := 2; i < len(args); i++ {
		h.db.listData[key] = append([]string{args[i].String()}, h.db.listData[key]...)
	}

	// If there are clients blocked on a BLPOP, send first elem through the first channel
	if waitChans, ok := h.db.blpopWaiters[key]; ok {
		popped := h.db.listData[key][0]
		waitChans[0] <- popped // Send the popped element through the channel to the client who called BLPOP first
		close(waitChans[0])    // Close the channel

		h.db.blpopWaiters[key] = h.db.blpopWaiters[key][1:] // Remove client off the queue
	}

	return EncodeToRESP(ConvertIntToRESP(len(h.db.listData[key])))

}

func (h *CommandHandler) HandleLRANGE(args []*RESPData) ([]byte, bool) {
	if len(args) != 4 {
		return nil, false
	}

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// If list doesn't exist, return empty array
	val, ok := h.db.GetList(string(args[1].Data))
	if !ok {
		return respEmptyArr, true
	}

	arrLen := len(val)
	// Parse start and stop indices, handling negative indices and out-of-bounds cases
	start, _ := strconv.Atoi(args[2].String())
	if start < 0 {
		start = arrLen + start
		start = max(0, start)
	}
	stop, _ := strconv.Atoi(string(args[3].Data))
	if stop < 0 {
		stop = arrLen + stop
		stop = max(0, stop)
	}
	stop = min(stop, arrLen-1)
	if start >= arrLen || start < 0 || stop < start || arrLen == 0 {
		return respEmptyArr, true
	}

	return EncodeToRESP(convertListToRESP(val[start : stop+1]))

}

func (h *CommandHandler) HandleLLEN(args []*RESPData) ([]byte, bool) {
	if len(args) != 2 {
		return nil, false
	}

	arrName := args[1].String()

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// If list doesn't exist, return 0
	arrResp, ok := h.db.GetList(arrName)
	if !ok  {
		return EncodeToRESP(ConvertIntToRESP(0))
	}

	// Return length of list
	return EncodeToRESP(ConvertIntToRESP(len(arrResp)))

}

func (h *CommandHandler) HandleBLPOP(args []*RESPData) ([]byte, bool) {
	if len(args) != 3 {
		return nil, false
	}

	// Get the name of the array to pop from
	key := args[1].String()

	// Make sure duration is a non-negative double float
	duration, err := strconv.ParseFloat(string(args[2].Data), 64)
	if err != nil || duration < 0 {
		return nil, false
	}

	h.db.mu.Lock()

	data, ok := h.db.listData[key]

	// If the array exists and has elements, pop and return immediately
	if ok && len(data) > 0 {
		poppedVal := data[0]
		h.db.listData[key] = data[1:]
		h.db.mu.Unlock()
		return EncodeToRESP(convertListToRESP([]string{key, poppedVal}))
	}

	// Add client to list of blocked clients on this array
	if _, ok = h.db.blpopWaiters[key]; !ok {
		h.db.blpopWaiters[key] = make([]chan string, 0)
	}
	c := make(chan string)
	h.db.blpopWaiters[key] = append(h.db.blpopWaiters[key], c)
	h.db.mu.Unlock() // Make sure to remove mutex so other clients can modify DB while this one is blocked

	// If duration is 0, block indefinitely until channel receives value
	if duration == 0.0 {
		poppedVal := <-c
		h.db.mu.Lock()
		defer h.db.mu.Unlock()
		h.db.listData[key] = h.db.listData[key][1:] // Remove the popped element from the array
		return EncodeToRESP(convertListToRESP([]string{key, poppedVal}))
	}

	// Otherwise block until channel receives value or timeout occurs
	select {

	case <-time.After(time.Duration(duration * float64(time.Second))):
		// Timeout occurred, remove client from waiters list
		h.db.mu.Lock()
		defer h.db.mu.Unlock()
		// Remove channel from waiters list
		waitChans := h.db.blpopWaiters[key]
		for i, chanVal := range waitChans {
			if chanVal == c {
				h.db.blpopWaiters[key] = append(waitChans[:i], waitChans[i+1:]...)
				break
			}
		}
		return respNullArr, true

	case poppedVal := <-c:
		// Successfully received popped value
		h.db.mu.Lock()
		defer h.db.mu.Unlock()
		h.db.listData[key] = h.db.listData[key][1:] // Remove the popped element from the array
		return EncodeToRESP(convertListToRESP([]string{key, poppedVal}))

	}

}

func (h *CommandHandler) HandleLPOP(args []*RESPData) ([]byte, bool) {
	if len(args) != 2 && len(args) != 3 {
		return nil, false
	}
	arrName := args[1].String()

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// Return null string if array doesn't exist
	arrResp, ok := h.db.GetList(arrName)
	if !ok {
		return respNullString, true
	}

	// Parse optional number of elements to remove
	numToRemove := 1
	if len(args) == 3 {
		parsedNum, err := strconv.Atoi(args[2].String())
		if err != nil || parsedNum < 0 {
			return nil, false
		}
		numToRemove = min(parsedNum, len(arrResp))
	}

	// Populate list of popped elements
	ret := make([]string, 0)
	ret = append(ret, arrResp[:numToRemove]...)

	// Update the array in the DB after popping elements
	if numToRemove == len(arrResp) {
		h.db.listData[arrName] = make([]string, 0)
	} else {
		h.db.listData[arrName] = arrResp[numToRemove:]
	}

	// Return single element if only one was removed
	if numToRemove == 1 {
		return EncodeToRESP(ConvertBulkStringToRESP(ret[0]))
	}
	// Return array of removed elements otherwise
	return EncodeToRESP(convertListToRESP(ret))

}

func (h *CommandHandler) HandleTYPE(args []*RESPData) ([]byte, bool) {
	if len(args) != 2 {
		return nil, false
	}

	h.db.mu.Lock()
	defer h.db.mu.Unlock()
	_, isString := h.db.GetString(args[1].String());
	if isString {
		return []byte("+string\r\n"), true
	}
	_, isList := h.db.GetList(args[1].String());
	if isList {
		return []byte("+list\r\n"), true
	}

	_, isStream := h.db.GetStream(args[1].String());
	if isStream {
		return []byte("+stream\r\n"), true
	}

	return respNoneString, true
}

func (h *CommandHandler) HandleXADD(args []*RESPData) ([]byte, bool) {
	if len(args) < 5 || len(args) % 2 == 0 {
		return nil, false
	}

	sname := args[1].String()
	

	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// Check if stream doesn't already exists
	stream, ok := h.db.GetStream(sname)

	// Make sure we can make a stream with the specified name
	if !ok && !h.db.CanSetStream(sname) {
		return nil, false
	} else if !ok {
		stream = make([]*StreamEntry, 0)
	}

	id := args[2].String()

	// Validate ID

	// Cannot be 0-0
	if id == "0-0" {
		return []byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"), true
	// Edge case: no previous entries and millis is 0
	} else if id == "0-*" && len(stream) == 0 {
		id = "0-1"
	// If ID is *, generate new ID based on previous entry (if exists)
	} else if id == "*" && len(stream) == 0 {
		id = fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0)
	} else if id == "*" {
		id = generateNewStreamID(stream[len(stream)-1].id)
	// Make sure ID is in the format int-int or int-*
	} else if idParts := strings.Split(id, "-"); len(idParts) != 2 {
		return nil, false
	} else if millis, err1 := strconv.Atoi(idParts[0]); err1 != nil {
		return nil, false
	} else if seqNum, err2 := strconv.Atoi(idParts[1]); err2 != nil && idParts[1] != "*" {
		return nil, false
	// Make sure millis is greater than or equal to previous entry's millis
	} else if len(stream) > 0 && millis < stream[len(stream)-1].GetMillis() {
		return []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"), true
	// Handle int-* case
	} else if idParts[1] == "*" && len(stream) > 0 && millis == stream[len(stream)-1].GetMillis() {
		id = fmt.Sprintf("%d-%d", millis, stream[len(stream)-1].GetSeqNum()+1)
	} else if idParts[1] == "*" {
		id = fmt.Sprintf("%d-0", millis)
	// Make sure seqNum is greater than previous entry's seqNum if millis are equal
	} else if len(stream) > 0 && seqNum <= stream[len(stream)-1].GetSeqNum() && millis == stream[len(stream)-1].GetMillis() {
		return []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"), true
	} else {
		// ID is valid, do nothing
	} 


	// Populate the stream entry to be added to the stream
	entry := &StreamEntry{id: id, values: make(map[string]string)}
	for i := 3; i < len(args); i += 2 {
		key := args[i].String()
		val := args[i+1].String()
		entry.values[key] = val
	}

	// Update stream in DB
	stream = append(stream, entry)
	h.db.SetStream(sname, stream)

	// Notify all relevant XREAD waiters of newly appended entries


	var notifyChans []chan []*StreamEntry
	for waiterId, chans := range h.db.xreadIdWaiters[sname] {
		if CompareStreamIDs(id, waiterId) == 1 {
			notifyChans = append(notifyChans, chans...)
		}
	}

	notifyChans = append(notifyChans, h.db.xreadAllWaiters[sname]...)

	h.db.mu.Unlock()
	for _, ch := range notifyChans {
		select { case ch <- stream: default: }
	}
	h.db.mu.Lock()

	// Return the ID of the stream that was just added
	return EncodeToRESP(ConvertBulkStringToRESP(id))
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

func (h *CommandHandler) HandleXRANGE(args []*RESPData) ([]byte, bool) {
	if len(args) != 4 {
		return nil, false
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
			return nil, false
		}

		millis1, err1 := strconv.Atoi(id1Parts[0])
		millis2, err2 := strconv.Atoi(id2Parts[0])
		if err1 != nil || err2 != nil {
			return nil, false
		}

		seqNum1, seqNum2 := 0, math.MaxInt
		if len(id1Parts) == 2 {
			seqNum1, err1 = strconv.Atoi(id1Parts[1])
		}
		if len(id2Parts) == 2 {
			seqNum2, err2 = strconv.Atoi(id2Parts[1])
		}

		if err1 != nil || err2 != nil{
			return nil, false
		}
		id1 = fmt.Sprintf("%d-%d", millis1, seqNum1)
		id2 = fmt.Sprintf("%d-%d", millis2, seqNum2)
	
	}


	ret := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 0)}
	
	// If first ID is greater than second, return empty list right away
	if CompareStreamIDs(id1, id2) == 1 {
		return EncodeToRESP(ret)
	}


	h.db.mu.Lock()
	defer h.db.mu.Unlock()

	// Check if stream exists
	stream, ok := h.db.GetStream(sname)
	if !ok {
		return nil, false
	}

	i := 0
	// Find first stream element in range
	for ; i < len(stream) && CompareStreamIDs(stream[i].id, id1) == -1; i++ {
		
	}

	// Add elements that are in range
	for ; i < len(stream) && CompareStreamIDs(stream[i].id, id2) != 1; i++ {
		respListEntry := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 2)}

		// Add ID as first element of list
		respStreamId := &RESPData{Type: BulkString, Data: []byte(stream[i].id)}
		respListEntry.ListRESPData[0] = respStreamId

		// Add list of keys & values as second element of list
		respKVList := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 0)}
		for k := range stream[i].values {
			respKVList.ListRESPData = append(respKVList.ListRESPData, ConvertBulkStringToRESP(k))
			respKVList.ListRESPData = append(respKVList.ListRESPData, ConvertBulkStringToRESP(stream[i].values[k]))
		}
		respListEntry.ListRESPData[1] = respKVList

		// Append entry to return list
		ret.ListRESPData = append(ret.ListRESPData, respListEntry)
	}

	return EncodeToRESP(ret)

}

func (h *CommandHandler) HandleXREAD(args []*RESPData) ([]byte, bool) {
	if len(args) < 4 || len(args) % 2 == 1 || (strings.ToLower(args[1].String()) != "streams" && strings.ToLower(args[1].String()) != "block") {
		return nil, false
	} else if strings.ToLower(args[1].String()) == "block" && strings.ToLower(args[3].String()) != "streams" {
		return nil, false
	}

	blocking := false
	blockDurationMillis := 0.0
	if strings.ToLower(args[1].String()) == "block" {
		blocking = true
		converted, err := strconv.ParseFloat(args[2].String(), 64)
		if err != nil || converted < 0 {
			return nil, false
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



	// Validate each ID
	for i := firstIdIndex; i < lastIdIndex+1; i++ {
		id := args[i].String()

		// ID can be $
		if id == "$" {
			continue
		}

		// Otherwise make sure it follows the format int-int
		idParts := strings.Split(id, "-")
		if len(idParts) != 2 {
			return nil, false
		}
		if _, err := strconv.Atoi(idParts[0]); err != nil {
			return nil, false
		}
		if _, err := strconv.Atoi(idParts[1]); err != nil {
			return nil, false
		}
	}
	

	h.db.mu.Lock()


	// For each stream, check if it exists
	for i := firstStreamIndex; i <= lastStreamIndex; i++ {
		if _, ok := h.db.GetStream(args[i].String()); !ok {
			return nil, false
		}
	}

	type WaitChanResult struct {
		streamKey string;
		results []*StreamEntry
	}

	// debugging
	fmt.Println("Number of streams to read from:", numStreams)
	fmt.Println("Duration to block (ms):", blockDurationMillis)
	fmt.Println("Blocking:", blocking)

	results := make(chan *WaitChanResult, numStreams)

	for i := firstStreamIndex; i <= lastStreamIndex; i++ {
		sname := args[i].String()
		id := args[i + numStreams].String()
		stream, _ := h.db.GetStream(sname)

		h.db.mu.Unlock()
		go func() {
			res := &WaitChanResult{streamKey: sname, results: make([]*StreamEntry, 0)}

			// If this isn't a blocking call, immediately send the relevant stream entries
			// If this is a blocking call and the stream isn't empty and contains relevant elements, return with the relevant stream entries
			if !blocking || (blocking && len(stream) > 0){
				for i := len(stream)-1; i >= 0 && CompareStreamIDs(stream[i].id, id) == 1; i-- {
					res.results = append(res.results, stream[i])
				}
				if !blocking || len(results) > 0 {
					results <- res
					return
				}
				
			}
			// Otherwise, create a channel of type []*StreamEntry
			receiver := make(chan []*StreamEntry)

			h.db.mu.Lock()

			// If id = "$", add to list of channels under stream key of xReadAllWaiters
			if id == "$" {
				_, ok := h.db.xreadAllWaiters[sname]
				if !ok {
					h.db.xreadAllWaiters[sname] = make([]chan []*StreamEntry, 0)
				}
				
				h.db.xreadAllWaiters[sname] = append(h.db.xreadAllWaiters[sname], receiver)
				// Make sure to remove channel from waiters list once done
				defer func() {
					h.db.mu.Lock()
					defer h.db.mu.Unlock()
					for i, ch := range h.db.xreadAllWaiters[sname] {
						if ch == receiver {
							h.db.xreadAllWaiters[sname] = append(h.db.xreadAllWaiters[sname][:i], h.db.xreadAllWaiters[sname][i+1:]...)
							break
						}
					}
				}()
			
			// Otherwise add to list of channels under id key under stream key of xReadIdWaiters
			} else {
				_, ok := h.db.xreadIdWaiters[sname]
				if !ok {
					h.db.xreadIdWaiters[sname] = make(map[string]([]chan []*StreamEntry))
				}
				_, ok = h.db.xreadIdWaiters[sname][id]
				if !ok {
					h.db.xreadIdWaiters[sname][id] = make([]chan []*StreamEntry, 0)
				}
				//debugging
				fmt.Println("Adding waiter for stream:", sname, " with ID:", id)
				h.db.xreadIdWaiters[sname][id] = append(h.db.xreadIdWaiters[sname][id], receiver)

				// Make sure to remove channel from waiters list once done
				defer func() {
					h.db.mu.Lock()
					defer h.db.mu.Unlock()
					for i, ch := range h.db.xreadIdWaiters[sname][id] {
						if ch == receiver {
							h.db.xreadIdWaiters[sname][id] = append(h.db.xreadIdWaiters[sname][id][:i], h.db.xreadIdWaiters[sname][id][i+1:]...)
							break
						}
					}
				}()
			}

			h.db.mu.Unlock()
			// If duration = 0, block indefinitely
			if blockDurationMillis == 0 {
				xaddResults := <-receiver
				res.results = xaddResults
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
				case xaddResults := <-receiver:
					// debugging
					fmt.Println("Woke up from blocking XREAD on stream:", sname)
					fmt.Println("Number of new entries received:", len(xaddResults))
					fmt.Println("ID:", xaddResults[0].id)
					res.results = xaddResults
					results <- res
					return
			}
			
		}()
	}
	

	ret := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 0)}
	for i := 0; i < numStreams; i++ {
		waitChanRes := <- results
		if len(waitChanRes.results) == 0 {
			continue
		}
		streamResults := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 2)}
		streamResults.ListRESPData[0] = ConvertBulkStringToRESP(waitChanRes.streamKey)
		streamResultIds := &RESPData{Type: Array, ListRESPData: make([]*RESPData, 0)} 
		

		for _, res := range waitChanRes.results {
			streamResultIds.ListRESPData = append(streamResultIds.ListRESPData, res.RESPData())
		}
		streamResults.ListRESPData[1] = streamResultIds

		ret.ListRESPData = append(ret.ListRESPData, streamResults)
	}

	return EncodeToRESP(ret)
}

func CompareStreamIDs(idA string, idB string) (int) {
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

func (h *CommandHandler) Handle(message []byte) ([]byte, bool) {
	_, respData, success := DecodeFromRESP(message)
	if !success || respData.Type != Array {
		fmt.Println("Unable to parse RESP request")
		return nil, false
	}

	request := respData.ListRESPData
	if len(request) == 0 {
		return nil, false
	}

	command := string(request[0].Data)
	switch strings.ToLower(command) {
	case "echo":
		return h.HandleECHO(request)
	case "ping":
		return h.HandlePING()
	case "set":
		return h.HandleSET(request)
	case "get":
		return h.HandleGET(request)
	case "rpush":
		return h.HandleRPUSH(request)
	case "lpush":
		return h.HandleLPUSH(request)
	case "lrange":
		return h.HandleLRANGE(request)
	case "llen":
		return h.HandleLLEN(request)
	case "lpop":
		return h.HandleLPOP(request)
	case "blpop":
		return h.HandleBLPOP(request)
	case "type":
		return h.HandleTYPE(request)
	case "xadd":
		return h.HandleXADD(request)
	case "xrange":
		return h.HandleXRANGE(request)
	case "xread":
		return h.HandleXREAD(request)
	default:
		return nil, false
	}
}
