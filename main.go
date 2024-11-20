package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	basePort          = 8000
	maxNodes          = 10
	httpPort          = 8080
	heartbeatInterval = 5 * time.Second
	leaderTimeout     = 10 * time.Second
)

type Node struct {
	ID              int
	Leader          bool
	lastKnownLeader int
	mutex           sync.RWMutex
}

var (
	globalLeader   int
	lastHeartbeat  time.Time
	heartbeatMutex sync.RWMutex
)

func main() {
	nodeID, _ := strconv.Atoi(os.Getenv("NODE_ID"))
	node := &Node{ID: nodeID, lastKnownLeader: 0}

	go listenForHeartbeats(node)
	go startHTTPServer(node)

	time.Sleep(5 * time.Second) // Wait for all nodes to start

	if !discoverExistingLeader(node) {
		electLeader(node)
	}

	for {
		if !isLeaderActive() {
			electLeader(node)
		} else {
			recognizeLeader(node)
		}

		node.mutex.RLock()
		isLeader := node.Leader
		node.mutex.RUnlock()

		if isLeader {
			sendHeartbeats(node)
			fmt.Printf("Node %d: Leader status: true\n", node.ID)
		}
		time.Sleep(heartbeatInterval)
	}
}

func discoverExistingLeader(node *Node) bool {
	for i := 1; i <= maxNodes; i++ {
		if pingNode(i) {
			leader, err := askForLeader(i)
			if err == nil && leader > 0 {
				globalLeader = leader
				node.mutex.Lock()
				node.Leader = (node.ID == leader)
				node.lastKnownLeader = leader
				node.mutex.Unlock()
				updateLastHeartbeat()
				fmt.Printf("Node %d: Discovered existing leader: Node %d\n", node.ID, leader)
				return true
			}
		}
	}
	return false
}

func askForLeader(nodeID int) (int, error) {
	resp, err := http.Get(fmt.Sprintf("http://node-%d:%d/leader", nodeID, httpPort))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var leader int
	_, err = fmt.Fscanf(resp.Body, "Current leader: Node %d", &leader)
	return leader, err
}

func recognizeLeader(node *Node) {
	lowestActiveID := maxNodes + 1
	for i := 1; i <= maxNodes; i++ {
		if pingNode(i) {
			if i < lowestActiveID {
				lowestActiveID = i
			}
			if i == globalLeader {
				// Current leader is still active
				updateLastHeartbeat()
				node.mutex.Lock()
				node.Leader = (node.ID == globalLeader)
				node.lastKnownLeader = globalLeader
				node.mutex.Unlock()
				return
			}
		}
	}

	// If we're here, the current leader is not responding
	if lowestActiveID <= maxNodes {
		node.mutex.Lock()
		node.Leader = (node.ID == lowestActiveID)
		node.lastKnownLeader = lowestActiveID
		node.mutex.Unlock()
		globalLeader = lowestActiveID
		updateLastHeartbeat()
		if node.Leader {
			fmt.Printf("Node %d: Assumed leadership\n", node.ID)
		} else {
			fmt.Printf("Node %d: Recognized leader: Node %d\n", node.ID, globalLeader)
		}
	}
}

func isLeaderActive() bool {
	heartbeatMutex.RLock()
	defer heartbeatMutex.RUnlock()
	return time.Since(lastHeartbeat) < leaderTimeout
}

func updateLastHeartbeat() {
	heartbeatMutex.Lock()
	lastHeartbeat = time.Now()
	heartbeatMutex.Unlock()
}

func electLeader(node *Node) {
	time.Sleep(2 * time.Second) // Short wait for stability

	if isLeaderActive() {
		recognizeLeader(node)
		return
	}

	lowestActiveID := node.ID
	for i := 1; i <= maxNodes; i++ {
		if i != node.ID && pingNode(i) {
			if i < lowestActiveID {
				lowestActiveID = i
			}
		}
	}

	node.mutex.Lock()
	node.Leader = (node.ID == lowestActiveID)
	node.lastKnownLeader = lowestActiveID
	node.mutex.Unlock()

	if node.Leader {
		globalLeader = node.ID
		updateLastHeartbeat()
		fmt.Printf("Node %d: Elected self as leader\n", node.ID)
	} else {
		fmt.Printf("Node %d: Recognized Node %d as leader\n", node.ID, lowestActiveID)
	}
}

func pingNode(id int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("node-%d:%d", id, basePort+id), time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func listenForHeartbeats(node *Node) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", basePort+node.ID))
	if err != nil {
		fmt.Printf("Error starting listener: %v\n", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		updateLastHeartbeat()
		conn.Close()
	}
}

func sendHeartbeats(node *Node) {
	for i := 1; i <= maxNodes; i++ {
		if i != node.ID {
			go func(id int) {
				if pingNode(id) {
					fmt.Printf("Node %d: Sent heartbeat to Node %d\n", node.ID, id)
				}
			}(i)
		}
	}
}

func startHTTPServer(node *Node) {
	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Current leader: Node %d\n", globalLeader)
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		node.mutex.RLock()
		isLeader := node.Leader
		lastKnownLeader := node.lastKnownLeader
		node.mutex.RUnlock()
		fmt.Fprintf(w, "Node %d - Is leader: %v, Last known leader: %d\n", node.ID, isLeader, lastKnownLeader)
	})

	fmt.Printf("Starting HTTP server on port %d\n", httpPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
		fmt.Printf("Error starting HTTP server: %v\n", err)
	}
}
