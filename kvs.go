package main

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/DistributedClocks/GoVector/govec/vclock"
	"github.com/labstack/echo/v4"
)

// Define JSON body for kvs PUT requests
type KVS_PUT_Request struct {
	Data           interface{} `json:"value"`
	Type           string      `json:"type"`
	CausalMetaData string      `json:"causal-metadata"`
	FromRepilca    string      `json:"from-replica,omitempty"`
}

// Define JSON body for kvs GET and DELETE requests
type KVS_GET_DELETE_Request struct {
	CausalMetaData string `json:"causal-metadata"`
	FromRepilca    string `json:"from-replica,omitempty"`
}

// PUT /kvs/<key>
// Add a key-value to the database
func putKey(c echo.Context) error {
	// Check which shard the key belongs to
	key := c.Param("key")
	keyByte := []byte(key)
	shardid := HASH_RING.LocateKey(keyByte).String()

	// If shardid is NOT the same as MY_SHARD_ID, then forward the request to the appropriate shard
	if shardid != MY_SHARD_ID {
		return forwardRequest(c, choseNodeFromShard(shardid))
	}

	// Read JSON from request body
	body, err := io.ReadAll(c.Request().Body)

	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}

	var input KVS_PUT_Request
	jsonErr := json.Unmarshal(body, &input)
	if jsonErr != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	// Validate key length
	if len(key) > 50 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Key is too long"})
	}

	// Check if the incoming value is empty
	if input.Data == nil || input.Data == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "PUT request does not specify a value"})
	}

	// Handle the causal metadata to ensure causal consistency
	var senderVC vclock.VClock
	// Check if request was sent from a client or another replica
	if input.FromRepilca != "" {
		// HANDLE REQUEST FROM ANOTHER REPLICA
		senderPos := input.FromRepilca
		// Parse causal metadata string from client
		senderVC, err = NewVClockFromString(input.CausalMetaData)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid metadata format"})
		}
		// Return error if senders VC value is not +1 receivers vc value
		if !(compareReplicasVC(senderVC, MY_VECTOR_CLOCK, senderPos)) {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Causal dependencies not satisfied; try again later"})
		}
		// Merge the replicas's vector clock with client vector clock
		MY_VECTOR_CLOCK.Merge(senderVC)
	} else {
		// HANDLE REQUEST FROM A CLIENT
		// Check if the client vector clock is nil
		if input.CausalMetaData != "" {
			// Parse causal metadata string from client
			senderVC, err = NewVClockFromString(input.CausalMetaData)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid metadata format"})
			}
			// Check if clients request is deliverable based on its vector clock
			// if recieverVC ---> clientVc return error
			// If the replica is less updated than the client, it cant deliver the message
			if !(senderVC.Compare(MY_VECTOR_CLOCK, vclock.Concurrent) || senderVC.Compare(MY_VECTOR_CLOCK, vclock.Equal)) {
				return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Causal dependencies not satisfied; try again later"})
			}
		}
		// Merge the replicas's vector clock with client vector clock
		MY_VECTOR_CLOCK.Merge(senderVC)
		// Increment replica's index in the vector clock to track a new write
		MY_VECTOR_CLOCK.Tick(SOCKET_ADDRESS)
		// Otherwise broadcast request to other replicas and deliver
		input.FromRepilca = SOCKET_ADDRESS
		input.CausalMetaData = MY_VECTOR_CLOCK.ReturnVCString()
		jsonData, _ := json.Marshal(input)
		go broadcast("PUT", "kvs/"+key, jsonData)
	}

	// Check if the key existed before the update
	_, existed := KVStore[key]

	// Update or create key-value mapping
	KVStore[key] = Value{input.Data, input.Type}

	// Return response with the appropriate status
	if existed {
		return c.JSON(http.StatusOK, map[string]string{"result": "replaced", "causal-metadata": MY_VECTOR_CLOCK.ReturnVCString()})
	}
	return c.JSON(http.StatusCreated, map[string]string{"result": "created", "causal-metadata": MY_VECTOR_CLOCK.ReturnVCString()})
}

// GET /kvs/<key>
// Return the value of the indicated key
func getKey(c echo.Context) error {
	// Check which shard the key belongs to
	key := c.Param("key")
	keyByte := []byte(key)
	shardid := HASH_RING.LocateKey(keyByte).String()

	// If shardid is NOT the same as MY_SHARD_ID, then forward the request to the appropriate shard
	if shardid != MY_SHARD_ID {
		return forwardRequest(c, choseNodeFromShard(shardid))
	}

	// Read JSON from request body
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}

	var input KVS_GET_DELETE_Request
	jsonErr := json.Unmarshal(body, &input)
	if jsonErr != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}

	// Handle the causal metadata to ensure causal consistency
	var senderVC vclock.VClock
	// HANDLE REQUEST FROM A CLIENT
	// Check if the client vector clock is nil
	if input.CausalMetaData != "" {
		// Parse causal metadata string from client
		senderVC, err = NewVClockFromString(input.CausalMetaData)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid metadata format"})
		}
		// Check if clients request is deliverable based on its vector clock
		// if recieverVC ---> clientVc return error
		// If the replica is less updated than the client, it cant deliver the message
		if !(senderVC.Compare(MY_VECTOR_CLOCK, vclock.Concurrent) || senderVC.Compare(MY_VECTOR_CLOCK, vclock.Equal)) {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Causal dependencies not satisfied; try again later"})
		}
	}

	// Check if key exists
	value, ok := KVStore[key]
	if !ok {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "Key does not exist"})
	}

	// Return response with original data type
	return c.JSON(http.StatusOK, map[string]interface{}{
		"result":          "found",
		"value":           value.Data,
		"causal-metadata": MY_VECTOR_CLOCK.ReturnVCString(),
	})
}

// DELETE /kvs/<key>
// Delete the indicateed key from the database
func deleteKey(c echo.Context) error {
	// Check which shard the key belongs to
	key := c.Param("key")
	keyByte := []byte(key)
	shardid := HASH_RING.LocateKey(keyByte).String()

	// If shardid is NOT the same as MY_SHARD_ID, then forward the request to the appropriate shard
	if shardid != MY_SHARD_ID {
		return forwardRequest(c, choseNodeFromShard(shardid))
	}

	// Read JSON from request body
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}

	var input KVS_GET_DELETE_Request
	jsonErr := json.Unmarshal(body, &input)
	if jsonErr != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}

	// Handle the causal metadata to ensure causal consistency
	var senderVC vclock.VClock
	// Check if request was sent from a client or another replica
	if input.FromRepilca != "" {
		// HANDLE REQUEST FROM ANOTHER REPLICA
		senderPos := input.FromRepilca
		// Parse causal metadata string from client
		senderVC, err = NewVClockFromString(input.CausalMetaData)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid metadata format"})
		}
		// Return error if senders VC value is not +1 receivers vc value
		if !(compareReplicasVC(senderVC, MY_VECTOR_CLOCK, senderPos)) {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Causal dependencies not satisfied; try again later"})
		}
		// Merge the replicas's vector clock with client vector clock
		MY_VECTOR_CLOCK.Merge(senderVC)
	} else {
		// HANDLE REQUEST FROM A CLIENT
		// Check if the client vector clock is nil
		if input.CausalMetaData != "" {
			// Parse causal metadata string from client
			senderVC, err = NewVClockFromString(input.CausalMetaData)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid metadata format"})
			}
			// Check if clients request is deliverable based on its vector clock
			// if recieverVC ---> clientVc return error
			// If the replica is less updated than the client, it cant deliver the message
			if !(senderVC.Compare(MY_VECTOR_CLOCK, vclock.Concurrent) || senderVC.Compare(MY_VECTOR_CLOCK, vclock.Equal)) {
				return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Causal dependencies not satisfied; try again later"})
			}
		}
		// Merge the replicas's vector clock with client vector clock
		MY_VECTOR_CLOCK.Merge(senderVC)
		// Increment replica's index in the vector clock to track a new write
		MY_VECTOR_CLOCK.Tick(SOCKET_ADDRESS)
		// Otherwise broadcast request to other replicas and deliver
		input.FromRepilca = SOCKET_ADDRESS
		input.CausalMetaData = MY_VECTOR_CLOCK.ReturnVCString()
		jsonData, _ := json.Marshal(input)
		go broadcast("PUT", "kvs/"+key, jsonData)
	}

	// Check if key exists
	_, ok := KVStore[key]
	if !ok {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "Key does not exist"})
	}

	// Delete key
	delete(KVStore, key)

	// Return response
	return c.JSON(http.StatusOK, map[string]string{"result": "deleted", "causal-metadata": MY_VECTOR_CLOCK.ReturnVCString()})
}
