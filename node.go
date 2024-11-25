package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type NeighborMessage struct {
	Neighbors []int `json:"neighbors"`
}

var neighbors []int

// Notify handler to receive neighbors
func notifyHandler(w http.ResponseWriter, r *http.Request) {
	var message NeighborMessage
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &message); err != nil {
		http.Error(w, "Failed to parse JSON", http.StatusBadRequest)
		return
	}
	neighbors = message.Neighbors
	fmt.Println("Received neighbors:", neighbors)
	w.WriteHeader(http.StatusOK)
}

// Multicast handler to propagate messages
func multicastHandler(w http.ResponseWriter, r *http.Request) {
	message, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	fmt.Printf("Message received: %s\n", message)

	// Forward the message to all neighbors
	for _, neighbor := range neighbors {
		url := fmt.Sprintf("http://node%d:8080/multicast", neighbor)
		resp, err := http.Post(url, "text/plain", r.Body)
		if err != nil {
			fmt.Printf("Failed to forward to node %d: %v\n", neighbor, err)
		} else {
			resp.Body.Close()
		}
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/notify", notifyHandler)
	http.HandleFunc("/multicast", multicastHandler)

	fmt.Println("Node is listening on port 8080")
	http.ListenAndServe(":8080", nil)
}
