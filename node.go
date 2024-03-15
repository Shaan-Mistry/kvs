package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/DistributedClocks/GoVector/govec/vclock"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Declare a global view of replicas
var CURRENT_VIEW []string

// Delare Vector Clock for enforcing causal consistency
var MY_VECTOR_CLOCK vclock.VClock

// Socket Address of the current View
var SOCKET_ADDRESS string

// Shard id that the current node belongs to
var MY_SHARD_ID string

// Protects access to CURRENT_VIEW
var viewMutex sync.Mutex

// KVStore represents the in-memory key-value store
var KVStore = make(map[string]Value)

// Value represents data with the original type
type Value struct {
	Data interface{}
	Type string
}

type Sync_Data struct {
	KvsSync        string `json:"kvsCopy"`
	VectorClockStr string `json:"vectorClock"`
	ShardsString   string `json:"shard"`
}

// GET /sync
func syncHandler(c echo.Context) error {
	// Prepare the data to be sent back to the requesting replica

	// Convert kvs map to JSON btes
	jsonBytes, err := json.Marshal(KVStore)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "Failed to convert kvs to string")
	}

	// Convert KVStore to string
	kvsString := string(jsonBytes)

	// Convert Shards to JSON bytes
	jsonBytes, err = json.Marshal(SHARDS)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "Failed to convert Shard Map to string")
	}

	// Convert Shards to string
	shardsString := string(jsonBytes)

	// Convert Vector Clock to string
	vcString := MY_VECTOR_CLOCK.ReturnVCString()

	syncData := Sync_Data{
		KvsSync:        kvsString,
		VectorClockStr: vcString,
		ShardsString:   shardsString,
	}

	// Send the current view and vector clock as a JSON response
	return c.JSON(http.StatusOK, syncData)
}

func main() {
	// Read environment variables
	SOCKET_ADDRESS = os.Getenv("SOCKET_ADDRESS")
	CURRENT_VIEW = strings.Split(os.Getenv("VIEW"), ",")
	SHARD_COUNT, err := strconv.Atoi(os.Getenv("SHARD_COUNT"))
	// Check if SHARD_COUNT was specified
	if err == nil {
		syncMyself(SHARD_COUNT)
		// Store my shard id
		updateMyShardID()
		// Create a hash ring to represent the distribution of shards
		HASH_RING = createHashRing()
	}
	// Define new Echo instance
	e := echo.New()
	fmt.Printf("\nMy ShardID: %s\n", MY_SHARD_ID)

	// Define Logger to display requests. Code from Echo Documentation
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogStatus:    true,
		LogURI:       true,
		LogMethod:    true,
		LogUserAgent: true,
		LogRemoteIP:  true,
		BeforeNextFunc: func(c echo.Context) {
			c.Set("vclock", MY_VECTOR_CLOCK.ReturnVCString())
		},
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			//vclock, _ := c.Get("vclock").(string)
			//method := strings.ToUpper(v.Method)
			if v.URI == "/view" {
				return nil
			}
			fmt.Printf("%v %s %v status: %v shard: %s  \n  shardMap: %v\n  view: %v\n\n", v.RemoteIP, v.Method, v.URI, v.Status, MY_SHARD_ID, SHARDS, CURRENT_VIEW)
			return nil
		},
	}))
	// Define /kvs GET endpoints
	e.GET("/kvs", getKey)
	e.GET("/kvs/", getKey)
	e.GET("/kvs/:key", getKey)
	// Define /kvs PUT endpoints
	e.PUT("/kvs", putKey)
	e.PUT("/kvs/", putKey)
	e.PUT("/kvs/:key", putKey)
	// Define /kvs DELETE endpoints
	e.DELETE("/kvs", deleteKey)
	e.DELETE("/kvs/", deleteKey)
	e.DELETE("/kvs/:key", deleteKey)
	// Define /view endpoints
	e.PUT("/view", putReplicaView)
	e.GET("/view", getView)
	e.DELETE("/view", deleteReplicaView)
	// Define /shard endpoints
	e.GET("/shard/ids", getAllShardIds)
	e.GET("/shard/node-shard-id", getMyShardId)
	e.GET("/shard/members/:id", getMembersOfShard)
	e.GET("/shard/key-count/:id", getShardKeyCount)
	e.PUT("/shard/add-member/:id", addNodeToShard)
	e.PUT("shard/reshard", reshard)
	e.PUT("/shard/kvs-update/:key", updateKvsForResharding)
	// Define /sync endpoint for syncing new nodes
	e.GET("/sync", syncHandler)
	// Build the JSON body to be sent: {"socket-address":"<IP:PORT>"}
	payload := map[string]string{"socket-address": SOCKET_ADDRESS}
	jsonPayload, _ := json.Marshal(payload)
	// Broadcaset Put View message to all replicas in the system
	broadcast("PUT", "view", jsonPayload, CURRENT_VIEW)
	// Start heartbeat checker
	go heartbeat()
	// Start Echo server
	e.Logger.Fatal(e.Start(SOCKET_ADDRESS))
}
