package main

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// How to organize Shards?
// Hash table? ex: {shard-id: [node1, node2]}
// Other requirements:
// Each shard must contain at least two nodes to provide fault tolerance
// Make sure that node sarrive to same sharding independentalty or through communication

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
