package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/labstack/echo/v4"
)

// Define SHARDS to store nodes to respective shard ids
var SHARDS = make(map[string][]string)

// Define hash ring to represent the distribution of shards
var HASH_RING *consistent.Consistent

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
		PartitionCount:    7,
		ReplicationFactor: 20,
		Load:              1.25,
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

	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
}

// GET /shard/node-shard-id
// Returns the shard identifier of this node
func getMyShardId(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
}

// GET /shard/members/<ID>
// Returns the members of the indicated shard
func getNodeShardID(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
}

// GET /shard/key-count/<ID>
// Returns the number of key-value pairs stored by the indicated shard
func getNumKvPairsInShard(c echo.Context) error {

	// Send request to node in indicated shard

	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
}

// PUT /shard/add-member/<ID>
// JSON body {"socket-address": <IP:PORT>}
// Assign the node <IP:PORT> to the shard <ID>
func addNodeToShard(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
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
	// Update or create key-value mapping
	KVStore[key] = Value{input.Data, input.Type}
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
	// fmt.Printf("\nResharding\n")
	// fmt.Printf("\nCurrent View: %v\n", CURRENT_VIEW)
	// fmt.Printf("\nCurrent Shards: %v\n", SHARDS)
	// Read JSON from request body
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

	// If we want to reduce the number of shards
	if targetNumShards < currNumShards {
		//numShardsToRemove := currNumShards - targetNumShards
		// Iteratively delete diff number of shards and distribute their nodes to other shards

		// If we want to increase the number of shards
	} else if targetNumShards > currNumShards {
		// Check if there are enough nodes to provide fault tolerance with the requested shard count
		if numNodes/targetNumShards < 2 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "Not enough nodes to provide fault tolerance with requested shard count"})
		}
		// Remove all nodes except 1 from each shard
		// Keep a list of nodes removed
		availableNodes := []string{}
		for shardid, nodes := range SHARDS {
			if len(nodes) > 1 {
				// Remove all nodes except 1 from the shard
				removedNodes := nodes[1:]
				SHARDS[shardid] = nodes[:1]
				// Add removed nodes to availableNodes
				availableNodes = append(availableNodes, removedNodes...)
			}
		}
		// Add new shards to SHARDS
		numShardsToAdd := targetNumShards - currNumShards
		for i := 0; i < numShardsToAdd; i++ {
			shardid := fmt.Sprintf("shard%d", i+currNumShards)
			SHARDS[shardid] = []string{}
			// Add a free node to each new shard
			SHARDS[shardid] = append(SHARDS[shardid], availableNodes[0])
			availableNodes = availableNodes[1:]
			// Add the new shard to the hash ring
			HASH_RING.Add(myMember(shardid))
		}

		// Update the key-value based on the new shard partitions
		// Go through each key and see if it needs to be moved to a different shard
		for key, value := range KVStore {
			// Check if the key belongs to the shard
			keyByte := []byte(key)
			shardid := HASH_RING.LocateKey(keyByte).String()
			// If the key does not belong to the shard, forward a Private PUT request to the appropriate shard
			if shardid != MY_SHARD_ID {
				// Build http method to send
				// Create the url using the current address
				url := fmt.Sprintf("http://%s/%s", choseNodeFromShard(shardid), "shard/kvs-update/"+key)
				payload := map[string]interface{}{"value": value.Data, "type": value.Type, "causal-metadata": "", "from-replica": SOCKET_ADDRESS}
				jsonBytes, _ := json.Marshal(payload)
				fmt.Println(string(jsonBytes))
				// Build the http request
				request, _ := http.NewRequest("PUT", url, bytes.NewBuffer(jsonBytes))
				// Forward the request to the appropriate shard
				fmt.Printf("\nForwarding request to %s\n", choseNodeFromShard(shardid))
				send(request)
			}

		}
		// Delete the key from the shard

		// Evenly distribute rest of the nodes back to the shards
		distributeNodesIntoShards(targetNumShards, availableNodes)

		// Sync the nodes within their shards

		// broadcast reshard to all nodes
		if input.FromRepilca == "" {
			payload := map[string]interface{}{"shard-count": targetNumShards, "from-replica": SOCKET_ADDRESS}
			jsonBytes, _ := json.Marshal(payload)
			broadcast("PUT", "shard/reshard", jsonBytes, CURRENT_VIEW)
		}

		//oldRing := HASH_RING
		// Update the hash ring
		//HASH_RING = createHashRing()
	}

	// fmt.Printf("\nFinished Resharding\n")
	// fmt.Printf("\nCurrent View: %v\n", CURRENT_VIEW)
	// fmt.Printf("\nCurrent Shards: %v\n", SHARDS)

	return c.JSON(http.StatusOK, map[string]string{"result": "resharded"})
}
