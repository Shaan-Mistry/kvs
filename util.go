package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/DistributedClocks/GoVector/govec/vclock"
	"github.com/labstack/echo/v4"
)

// Builds a new vector clock object given a string
func NewVClockFromString(vcStr string) (vclock.VClock, error) {
	// Initialize an empty map to hold the deserialized data
	vcMap := make(map[string]uint64)
	// Unmarshal the JSON string into the map
	err := json.Unmarshal([]byte(vcStr), &vcMap)
	// Create a new VClock object from the vcMap
	vc := vclock.New().CopyFromMap(vcMap)
	return vc, err
}

// Removes a replica address from CURRENT_VIEW
func removeFromView(address string) {
	viewMutex.Lock()         // Only one can modify CURRENT_VIEW at a time
	defer viewMutex.Unlock() // Make sure the lock is released at the end of the function
	for i, addr := range CURRENT_VIEW {
		if addr == address {
			CURRENT_VIEW = append(CURRENT_VIEW[:i], CURRENT_VIEW[i+1:]...)
			break
		}
	}
	// Remove from SHARDS
	for shardid, nodes := range SHARDS {
		for i, addr := range nodes {
			if addr == address {
				SHARDS[shardid] = append(nodes[:i], nodes[i+1:]...)
				break
			}
		}
	}
}

// Periodically check if a replica is still alive
func heartbeat() {
	client := &http.Client{Timeout: 5 * time.Second}
	time.Sleep(time.Second)
	for {
		time.Sleep(time.Second)
		viewMutex.Lock() // Lock before reading CURRENT_VIEW
		currentViewSnapshot := make([]string, len(CURRENT_VIEW))
		copy(currentViewSnapshot, CURRENT_VIEW) // Create a copy to iterate over
		viewMutex.Unlock()                      // Unlock after copying
		for _, address := range currentViewSnapshot {
			// Dont check youself
			if address == SOCKET_ADDRESS {
				continue
			}
			resp, err := client.Get(fmt.Sprintf("http://%s/view", address))
			if err != nil || resp.StatusCode != http.StatusOK {
				fmt.Printf("Replica at %s is down. Removing from current view.\n", address)
				removeFromView(address) // Safely remove the address
				// broadcast delete views
				payload := map[string]string{"socket-address": address}
				jsonPayload, _ := json.Marshal(payload)
				broadcast("DELETE", "view", jsonPayload, CURRENT_VIEW)
			}
		}
	}
}

// Send http requests till success or replica is down
func send(request *http.Request) {
	client := &http.Client{Timeout: 1 * time.Second}
	for {
		resp, err := client.Do(request)
		if err != nil {
			// Replica is down
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != 503 {
			return
		}
		// Sleep for 1 second and then try again
		time.Sleep(time.Second)
	}
}

// Broadcast a Request to all other replicas in the system asyncronously
func broadcast(method string, endpoint string, jsonData []byte, nodes []string) error {
	// Broadcast request to all replicas
	for _, address := range nodes {
		// Dont broacast to youself
		if address == SOCKET_ADDRESS {
			continue
		}
		// Create the url using the current address
		url := fmt.Sprintf("http://%s/%s", address, endpoint)
		// Build the http request
		request, err := http.NewRequest(method, url, bytes.NewBuffer(jsonData))
		request.RemoteAddr = address
		if err != nil {
			return err
		}
		// Send request to current replica
		go send(request)

	}
	return nil
}

// Checks the following condition
// VC[m][k] = VC[p][k] + 1,  where k is the sender’s position
// VC[m][k] <= VC[p][k]  ,   for every other k
func compareReplicasVC(senderVC, recieverVC vclock.VClock, senderPos string) bool {
	for id := range senderVC {
		senderTick, _ := senderVC.FindTicks(id)
		recieverTick, _ := recieverVC.FindTicks(id)
		if id == senderPos {
			if senderTick != recieverTick+1 {
				return false
			}
		} else {
			if senderTick > recieverTick {
				return false
			}
		}
	}
	return true
}

// Given a shard count and a list of nodes,
// distribute the nodes in the current view into shards
func distributeNodesIntoShards(shardCount int, nodes []string) {
	// Delegate all nodes in current view to SHARDS map
	nodesPerShard := len(nodes) / shardCount
	remainder := len(nodes) % shardCount
	// Create shardCount shardids in the SHARDS map
	for i := 0; i < shardCount; i++ {
		shardid := fmt.Sprintf("shard%d", i)
		// Assign len(nodes)/SHARD_COUNT nodes to each shard
		for j := 0; j < nodesPerShard; j++ {
			SHARDS[shardid] = append(SHARDS[shardid], nodes[nodesPerShard*i+j])
		}
	}
	// Distribute the remainder nodes to the first few shards
	for i := 0; i < remainder; i++ {
		shardid := fmt.Sprintf("shard%d", i)
		SHARDS[shardid] = append(SHARDS[shardid], nodes[nodesPerShard*shardCount+i])
	}
}

func syncMyself(shardCount int) {
	// Look for a node to sync with
	for _, address := range CURRENT_VIEW {
		// Dont sync with yourself
		if address == SOCKET_ADDRESS {
			continue
		}
		err := syncWithNode(address)
		if err == nil {
			return
		}
	}
	// If there is no nodes to sync with, initialize Vector Clock and SHARDS
	// Initialize Vector Clock
	MY_VECTOR_CLOCK = vclock.New()
	for _, address := range CURRENT_VIEW {
		MY_VECTOR_CLOCK.Set(address, 0)
	}
	// Distribute all nodes into shards
	distributeNodesIntoShards(shardCount, CURRENT_VIEW)
}

// Makes a request to existing replica to get the current view and vector clock
// Updates the new replica's state based on the response
func syncWithNode(targetReplicaAddress string) error {
	client := &http.Client{
		Timeout: 1 * time.Second, // Set a timeout to avoid hanging indefinitely
	}

	// Make the URL for the sync endpoint of the target replica
	reqURL := fmt.Sprintf("http://%s/sync", targetReplicaAddress)

	// Make a GET request to the sync endpoint
	resp, err := client.Get(reqURL)
	if err != nil {
		return fmt.Errorf("failed to fetch state from replica %s: %v", targetReplicaAddress, err)
	}
	defer resp.Body.Close()

	// Check if the response status code indicates success
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-OK response from replica %s: %s", targetReplicaAddress, resp.Status)
	}

	var syncData Sync_Data
	if err := json.NewDecoder(resp.Body).Decode(&syncData); err != nil {
		return fmt.Errorf("error decoding sync response: %v", err)
	}

	// Use NewVClockFromString to parse the vector clock string
	newVClock, err := NewVClockFromString(syncData.VectorClockStr)
	if err != nil {
		return fmt.Errorf("error creating vector clock from string: %v", err)
	}

	var newKVS map[string]Value
	err = json.Unmarshal([]byte(syncData.KvsSync), &newKVS)
	if err != nil {
		return fmt.Errorf("error creating kvs from string: %v", err)
	}

	var newShards map[string][]string
	err = json.Unmarshal([]byte(syncData.ShardsString), &newShards)
	if err != nil {
		return fmt.Errorf("error creating shards from string: %v", err)
	}

	// Update SHARDS with the new shards
	SHARDS = newShards

	// Directly update MY_VECTOR_CLOCK with the new vector clock
	MY_VECTOR_CLOCK = newVClock

	// Update KV Store with new KVS
	KVStore = newKVS

	fmt.Printf("Successfully synchronized with cluster via replica %s\n", targetReplicaAddress)
	return nil
}

// Stores which shard the current node belongs to into MY_SHARD_ID
func updateMyShardID() {
	// Look through all shardids in SHARDS values and find to find my SOCKET_ADDRESS
	for shardid, nodes := range SHARDS {
		for _, address := range nodes {
			if address == SOCKET_ADDRESS {
				MY_SHARD_ID = shardid
				return
			}
		}
	}
	MY_SHARD_ID = ""
}

// Chose a random node from the inputted shardid
func choseNodeFromShard(shardid string) string {
	// Add loop thing if its works then break or whatever
	nodes := SHARDS[shardid]
	return nodes[0]
}

// Forward the request to specified address
func forwardRequest(c echo.Context, address string) error {
	// Store HTTP method type (GET, PUT, DELETE)
	httpMethod := c.Request().Method
	// Store key
	key := c.Param("key")

	// Read the body of the incoming request
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to read request body")
	}
	// Create a new request to forward the Main Instance
	url := fmt.Sprintf("http://%s/kvs/%s", address, key)
	req, err := http.NewRequest(httpMethod, url, bytes.NewReader(body))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create forwarding request")
	}
	// Send the request to Main Instance using an http.Client
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, map[string]string{"error": "Cannot forward request"})
	}
	defer resp.Body.Close()
	// Forward the response from the Main Instance back to the Client
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to read response body from target service")
	}
	// Send the response back to the client
	return c.Blob(resp.StatusCode, "application/json", respBody)

}
