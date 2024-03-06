package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
}

// GET /sync
func syncHandler(c echo.Context) error {
	// Prepare the data to be sent back to the requesting replica

	// Convert kvs map to JSON string
	jsonString, err := json.Marshal(KVStore)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "Failed to convert kvs to string")
	}
	// Convert byte slice to string
	kvsString := string(jsonString)

	syncData := Sync_Data{
		KvsSync:        kvsString,
		VectorClockStr: MY_VECTOR_CLOCK.ReturnVCString(),
	}

	// Send the current view and vector clock as a JSON response
	return c.JSON(http.StatusOK, syncData)
}

func main() {
	// Read environment variables
	SOCKET_ADDRESS = os.Getenv("SOCKET_ADDRESS")
	CURRENT_VIEW = strings.Split(os.Getenv("VIEW"), ",")
	// Sync replica with the system
	err := syncMyself()
	if err != nil {
		MY_VECTOR_CLOCK = vclock.New()
		for _, address := range CURRENT_VIEW {
			MY_VECTOR_CLOCK.Set(address, 0)
		}
	}
	// Define new Echo instance
	e := echo.New()
	// Define Logger to display requests. Code from Echo Documentation
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogStatus: true,
		LogURI:    true,
		LogMethod: true,
		BeforeNextFunc: func(c echo.Context) {
			c.Set("vclock", MY_VECTOR_CLOCK.ReturnVCString())
			c.Set("table", KVStore)
		},
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			kvs := c.Get("table")
			vclock, _ := c.Get("vclock").(string)
			method := strings.ToUpper(v.Method)
			if method == "GET" && v.URI == "/view" {
				return nil
			}
			fmt.Printf("[%s] %v, status: %v, vclock: %v kvs: %v", method, v.URI, v.Status, vclock, kvs)
			println()
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
	e.GET("/shard/ids", nil)
	e.GET("/shard/node-shard-id", nil)
	e.GET("/shard/members/:id", nil)
	e.PUT("/shard/add-member/:id", nil)
	e.PUT("shard/reshard", nil)
	// Define /sync endpoint for syncing new nodes
	e.GET("/sync", syncHandler)
	// Build the JSON body to be sent: {"socket-address":"<IP:PORT>"}
	//payload := fmt.Sprintf("{socket-address:%s}", SOCKET_ADDRESS)
	payload := map[string]string{"socket-address": SOCKET_ADDRESS}
	jsonPayload, _ := json.Marshal(payload)
	// Broadcaset Put View message to all replicas in the system
	println("------------ Broadcasting Myself! ------------")
	broadcast("PUT", "view", jsonPayload)
	// Start heartbeat checker
	go heartbeat()
	// Start Echo server
	e.Logger.Fatal(e.Start(SOCKET_ADDRESS))
}
