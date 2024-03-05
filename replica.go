package main

import (
	"encoding/json"
	"fmt"
	"io"
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

// Define Valid JSON formats for KVS Requets
type KVS_PUT_Request struct {
	Data           interface{} `json:"value"`
	Type           string      `json:"type"`
	CausalMetaData string      `json:"causal-metadata"`
	FromRepilca    string      `json:"from-replica,omitempty"`
}

type KVS_GET_DELETE_Request struct {
	CausalMetaData string `json:"causal-metadata"`
	FromRepilca    string `json:"from-replica,omitempty"`
}

// Define Valid JSON format for View Requests
type View_Request struct {
	SocketAdress string `json:"socket-address"`
	FromRepilca  string `json:"from-replica,omitempty"`
}

type Sync_Data struct {
	KvsSync        string `json:"kvsCopy"`
	VectorClockStr string `json:"vectorClock"`
}

// PUT /kvs/<key>
func putKey(c echo.Context) error {
	key := c.Param("key")

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
func getKey(c echo.Context) error {
	key := c.Param("key")

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
func deleteKey(c echo.Context) error {
	key := c.Param("key")

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

// PUT /view
func putReplicaView(c echo.Context) error {
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}
	var viewRequest View_Request
	if err := json.Unmarshal(body, &viewRequest); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	for _, addr := range CURRENT_VIEW {
		if addr == viewRequest.SocketAdress {
			return c.JSON(http.StatusOK, map[string]string{"result": "already present"})
		}
	}
	CURRENT_VIEW = append(CURRENT_VIEW, viewRequest.SocketAdress)
	MY_VECTOR_CLOCK.Set(viewRequest.SocketAdress, 0)
	return c.JSON(http.StatusCreated, map[string]string{"result": "added"})
}

// GET /view
func getView(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string][]string{"view": CURRENT_VIEW})
}

// DELETE /view
func deleteReplicaView(c echo.Context) error {
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Failed to read request body"})
	}
	var viewRequest View_Request
	if err := json.Unmarshal(body, &viewRequest); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON format"})
	}
	for i, addr := range CURRENT_VIEW {
		if addr == viewRequest.SocketAdress {
			// Remove the address from the view
			CURRENT_VIEW = append(CURRENT_VIEW[:i], CURRENT_VIEW[i+1:]...)
			return c.JSON(http.StatusOK, map[string]string{"result": "deleted"})
		}
	}

	return c.JSON(http.StatusNotFound, map[string]string{"error": "View has no such replica"})
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
