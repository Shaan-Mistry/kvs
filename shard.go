package main

import (
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

// When to reshard?
// When a shard contains one node due to the failure of other nodes
// When adding new nodes to the system

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

// PUT /shard/reshard
// JSON body {"shard-count": <INTEGER>}
// Trigger a reshard into <INTEGER> shards
func reshard(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"result": "resharded"})
}
