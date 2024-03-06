package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/DistributedClocks/GoVector/govec/vclock"
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
}

// Periodically check if a replica is still alive
func heartbeat() {
	client := &http.Client{Timeout: 1 * time.Second}
	time.Sleep(time.Second)
	for {
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
				broadcast("DELETE", "view", jsonPayload)
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
			//println("[BROAD]", method, url)
			return
		}
		// Sleep for 1 second and then try again
		time.Sleep(time.Second)
	}
}

// Broadcast a Request to all other replicas in the system asyncronously
func broadcast(method string, endpoint string, jsonData []byte) error {
	// Broadcast request to all replicas
	for _, address := range CURRENT_VIEW {
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

func syncMyself() error {
	for _, address := range CURRENT_VIEW {
		err := syncWithCluster(address)
		if err == nil {
			return nil
		}
		println(err.Error())
	}
	return fmt.Errorf("failed to sync")
}

// Makes a request to existing replica to get the current view and vector clock
// Updates the new replica's state based on the response
func syncWithCluster(targetReplicaAddress string) error {
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

	// Directly update MY_VECTOR_CLOCK with the new vector clock
	MY_VECTOR_CLOCK = newVClock

	// Update KV Store with new KVS
	KVStore = newKVS

	fmt.Printf("Successfully synchronized with cluster via replica %s\n", targetReplicaAddress)
	return nil
}
