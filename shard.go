package main

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/labstack/echo/v4"
)

// Define SHARDS to store nodes to respective shard ids
var SHARDS = make(map[string][]string)

// Define hash ring to represent the distribution of shards
var HASH_RING *consistent.Consistent

// Define a lock to protext concurrent access to KVStore
var KVSmutex = &sync.Mutex{}

type myMember string

func (m myMember) String() string {
	return string(m)
}

// Define the consitent hashing algorithm/functions
// Taken from consistent.go documentation
// https://pkg.go.dev/github.com/buraksezer/consistent#section-readme
// Define hash function to be used by the consistent hashing algorithm
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

// Creates a new hash ring
func createHashRing() *consistent.Consistent {
	// Create a new consistent instance
	cfg := consistent.Config{
		PartitionCount:    11,
		ReplicationFactor: 5,
		Load:              1.10,
		Hasher:            hasher{},
	}
	hashRing := consistent.New(nil, cfg)
	for key := range SHARDS {
		hashRing.Add(myMember(key))
	}
	return hashRing
}

// Each shard must contain at least two nodes to provide fault tolerance
// Make sure that node arrive to same sharding independentalty or through communication
// Any node should be able to determine what shard a key belongs to, without having to query every
//shard for it

// GET /shard/ids
// Returns list of all shard indentifiers
func getAllShardIds(c echo.Context) error {
	shardIDs := make([]string, 0, len(SHARDS))
	for shardID := range SHARDS {
		shardIDs = append(shardIDs, shardID)
	}
	return c.JSON(http.StatusOK, map[string][]string{"shard-ids": shardIDs})
}

// GET /shard/node-shard-id
// Returns the shard identifier of this node
func getMyShardId(c echo.Context) error {
	if MY_SHARD_ID == "" {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Unable to determine shard ID for the current node"})
	}
	return c.JSON(http.StatusOK, map[string]string{"node-shard-id": MY_SHARD_ID})
}

// GET /shard/members/<ID>
// Returns the members of the indicated shard
func getMembersOfShard(c echo.Context) error {
	// Extracting the shard ID from the request URL parameter
	shardID := c.Param("id")

	// Checking if the shard ID exists in the SHARDS map
	if members, ok := SHARDS[shardID]; ok {
		// If found, return the members of the shard
		return c.JSON(http.StatusOK, map[string][]string{"shard-members": members})
	}
	// If the shard ID does not exist, return an error message
	return c.JSON(http.StatusNotFound, map[string]string{"error": "Shard ID not found"})
}

// GET /shard/key-count/<ID>
// Returns the number of key-value pairs stored by the indicated shard
func getShardKeyCount(c echo.Context) error {
	shardID := c.Param("id")
	// Check if the shard exists
	if _, exists := SHARDS[shardID]; !exists {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "Shard ID not found"})
	}

	// Check if this node belongs to the shard
	if shardID == MY_SHARD_ID {
		// Count the number of key-value pairs that belong to this shard
		count := 0
		for key := range KVStore {
			keyByte := []byte(key)
			if HASH_RING.LocateKey(keyByte).String() == shardID {
				count++
			}
		}
		return c.JSON(http.StatusOK, map[string]int{"shard-key-count": count})
	} else {
		// Forward the request to a node in the shard
		chosenNode := choseNodeFromShard(shardID)
		if chosenNode == "" {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "No node available in the shard"})
		}
		return forwardRequestToShard(c, chosenNode, "/shard/key-count/"+shardID)
	}
}
func forwardRequestToShard(c echo.Context, nodeAddress, endpoint string) error {
	// Create the full URL
	url := "http://" + nodeAddress + endpoint
	// Send request to node in indicated shard

	// Forward the GET request
	resp, err := http.Get(url)
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Failed to forward request"})
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to read response from node"})
	}

	// Directly forward the status code and response body received from the chosen node
	return c.JSONBlob(resp.StatusCode, body)
}

// Define a structure to parse the request body.
type addNodeRequest struct {
	SocketAddress string `json:"socket-address"`
	FromRepilca   string `json:"from-replica,omitempty"`
}

// PUT /shard/add-member/<ID>
// JSON body {"socket-address": <IP:PORT>}
// Assign the node <IP:PORT> to the shard <ID>
func addNodeToShard(c echo.Context) error {
	// Extract the shard ID from the URL parameter.
	shardID := c.Param("id")

	// Read JSON from request body
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}
	// Unmarshal JSON
	var input addNodeRequest
	jsonErr := json.Unmarshal(body, &input)
	if jsonErr != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	// Check if the shard exists
	if _, exists := SHARDS[shardID]; !exists {
		// If the shard doesn't exist, return a not found response.
		return c.JSON(http.StatusNotFound, map[string]string{"error": "Shard ID not found"})
	}
	// Add the node to the shard
	SHARDS[shardID] = append(SHARDS[shardID], input.SocketAddress)

	// If I am the node that is being added, sync myself with the shard I am assigned to
	if input.SocketAddress == SOCKET_ADDRESS {
		// Update my shard id in MY_SHARD_ID
		syncWithShard()
	}

	// If the request is not from anotehr replica, then broadcast the new addition to all other nodes
	if input.FromRepilca == "" {
		// Create JSON payload to be sent to other nodes
		payload := map[string]string{"socket-address": input.SocketAddress, "from-replica": SOCKET_ADDRESS}
		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, "Failed to convert JSON payload to string")
		}
		broadcast("PUT", "shard/add-member/"+shardID, jsonBytes, CURRENT_VIEW)
	}
	// Return a success response.
	return c.JSON(http.StatusOK, map[string]string{"result": "Node added to shard"})
}

// Define private endpoint for updating kvs for resharding
// PUT /shard/kvs-update/<key>
func updateKvsForResharding(c echo.Context) error {
	// Store key value
	key := c.Param("key")
	// Read JSON from request body
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}
	// Unmarshal JSON
	var input KVS_PUT_Request
	jsonErr := json.Unmarshal(body, &input)
	if jsonErr != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	// Lock before accessing the KVStore
	KVSmutex.Lock()
	// Update or create key-value mapping
	KVStore[key] = Value{input.Data, input.Type}
	// Unlock after accessing the KVStore
	KVSmutex.Unlock()
	// Return success
	return c.JSON(http.StatusOK, map[string]string{"result": "updated"})
}

// Define JSON body for kvs GET and DELETE requests
type Reshard_Request struct {
	ShardCount  int    `json:"shard-count"`
	FromRepilca string `json:"from-replica,omitempty"`
}

// PUT /shard/reshard
// JSON body {"shard-count": <INTEGER>}
// Trigger a reshard into <INTEGER> shards
func reshard(c echo.Context) error {
	body, err := io.ReadAll(c.Request().Body)

	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}

	var input Reshard_Request
	jsonErr := json.Unmarshal(body, &input)
	if jsonErr != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}

	numNodes := len(CURRENT_VIEW)
	currNumShards := len(HASH_RING.GetMembers())
	targetNumShards := input.ShardCount
	// Check if the number of shards is valid
	if targetNumShards < 1 || (targetNumShards > currNumShards && numNodes/targetNumShards < 2) {
		// Check if there are enough nodes to provide fault tolerance with the requested shard count
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Not enough nodes to provide fault tolerance with requested shard count"})
	}
	// Distribute nodes into shards
	distributeNodesIntoShards(targetNumShards, CURRENT_VIEW)
	// Update my shard id in MY_SHARD_ID
	updateMyShardID()
	// Update Hash Ring
	HASH_RING = createHashRing()
	// Go through each key and see if it needs to be moved to a different shard
	// Lock before accessing the KVStore
	KVSmutex.Lock()
	for key, value := range KVStore {
		// Check if the key belongs to the shard
		keyByte := []byte(key)
		shardid := HASH_RING.LocateKey(keyByte).String()
		// Forward a private PUT KVS request to the appropriate shard
		payload := map[string]interface{}{"value": value.Data, "causal-metadata": nil, "from-replica": SOCKET_ADDRESS}
		jsonBytes, _ := json.Marshal(payload)
		// Forward the request to the appropriate shard
		broadcastTest("PUT", "shard/kvs-update/"+key, jsonBytes, SHARDS[shardid])
		// Delete the shard from my KVStore
		if shardid != MY_SHARD_ID {
			delete(KVStore, key)
		}
	}
	// Unlock after accessing the KVStore
	KVSmutex.Unlock()
	// If request is not from another node, broadcast reshard to all nodes
	if input.FromRepilca == "" {
		input.FromRepilca = SOCKET_ADDRESS
		jsonBytes, _ := json.Marshal(input)
		broadcastTest("PUT", "shard/reshard", jsonBytes, CURRENT_VIEW)
	}
	// Return success
	return c.JSON(http.StatusOK, map[string]string{"result": "resharded"})
}
