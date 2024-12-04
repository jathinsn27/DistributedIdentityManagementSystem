package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	basePort          = 8000
	maxNodes          = 4
	httpPort          = 8080
	heartbeatInterval = 2 * time.Second
	leaderTimeout     = 4 * time.Second
)

type Node struct {
	ID              int
	Leader          bool
	lastKnownLeader int
	mutex           sync.RWMutex
	activeNodes     map[int]bool
	votes           map[int]bool
	term            int
	address         string
	membershipHost  string
}

type SpanningTreeNode struct {
	ID       string
	address  string
	Parent   *SpanningTreeNode
	Children []*SpanningTreeNode
	mu       sync.RWMutex
}

type SpanningTree struct {
	Root *SpanningTreeNode
	mu   sync.RWMutex
}

type MemberInfo1 struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	LeaseID   int64     `json:"lease_id"`
	ExpiresAt time.Time `json:"expires_at"`
	IsLeader  bool      `json:"is_leader"`
}

type Message struct {
	Type        string      // "VoteRequest" or "Heartbeat"
	VoteRequest VoteRequest // Used if Type is "VoteRequest"
	Heartbeat   struct {    // Used if Type is "Heartbeat"
		Term   int
		Leader int
	}
}

type VoteRequest struct {
	CandidateID int
	Term        int
}

type VoteResponse struct {
	VoteGranted bool
	Term        int
}

func InitGlobalTree() {
	treeOnce.Do(func() {
		globalTree = &SpanningTree{
			Root: nil,
			mu:   sync.RWMutex{},
		}
	})
}

// GetGlobalTree returns the global tree, initializing it if necessary
func GetGlobalTree() *SpanningTree {
	InitGlobalTree()
	return globalTree
}

var (
	lastHeartbeat      time.Time
	heartbeatMutex     sync.RWMutex
	globalTree         *SpanningTree
	treeOnce           sync.Once
	prevMembershipList []string
)

func main() {
	nodeID, _ := strconv.Atoi(os.Getenv("NODE_ID"))
	membershipHost := os.Getenv("MEMBERSHIP_HOST")

	node := &Node{
		ID:             nodeID,
		activeNodes:    make(map[int]bool),
		votes:          make(map[int]bool),
		term:           0,
		address:        fmt.Sprintf("node-%d:8080", nodeID),
		membershipHost: membershipHost,
	}

	err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Register with membership service
	if err := registerWithMembership(node); err != nil {
		log.Fatal(err)
	}

	go listenForHeartbeats(node)
	go startHTTPServer(node)

	// Monitor membership changes and update active nodes list dynamically
	go monitorMembershipChanges(node)

	go sendHeartbeatToMembership(node)

	time.Sleep(5 * time.Second)

	if !discoverExistingLeader(node) {
		startElection(node)
	}

	for {
		fmt.Printf("This is my %d", nodeID)
		if !isLeaderActive() {
			startElection(node)
		} else {
			recognizeLeader(node)
		}

		node.mutex.RLock()
		isLeader := node.Leader
		node.mutex.RUnlock()

		if isLeader {
			sendHeartbeats(node)
		}

		time.Sleep(heartbeatInterval)
	}
}

func registerWithMembership(node *Node) error {
	info := struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}{
		ID:      strconv.Itoa(node.ID),
		Address: node.address,
	}

	body, _ := json.Marshal(info)

	_, err := http.Post(
		fmt.Sprintf("http://%s/register", node.membershipHost),
		"application/json",
		bytes.NewBuffer(body),
	)

	return err
}

func sendHeartbeatToMembership(node *Node) {
	// Send periodic heartbeats to the membership service to indicate this node is alive.
	ticker := time.NewTicker(heartbeatInterval)
	for range ticker.C {
		info := struct {
			ID       string `json:"id"`
			Address  string `json:"address"`
			IsLeader bool   `json:"is_leader"`
		}{
			ID:       strconv.Itoa(node.ID),
			Address:  node.address,
			IsLeader: node.Leader,
		}
		body, _ := json.Marshal(info)
		http.Post(
			fmt.Sprintf("http://%s/keepalive", node.membershipHost),
			"application/json",
			bytes.NewBuffer(body),
		)
	}
}

func startElection(node *Node) {
	time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	node.mutex.Lock()

	if node.lastKnownLeader > 0 && node.activeNodes[node.lastKnownLeader] {
		node.mutex.Unlock()
		return
	}

	if time.Since(lastHeartbeat) < leaderTimeout {
		node.mutex.Unlock()
		return
	}

	for i := 1; i < node.ID; i++ {
		if node.activeNodes[i] {
			node.mutex.Unlock()
			return
		}
	}

	node.term++
	currentTerm := node.term

	node.votes = make(map[int]bool)
	node.votes[node.ID] = true

	node.mutex.Unlock()

	votes := 1

	votingComplete := make(chan bool)

	go func() {
		time.Sleep(2 * time.Second)
		votingComplete <- true
	}()

	for id := range node.activeNodes {
		if id != node.ID {
			go func(targetID int) {
				if requestVote(node, targetID, currentTerm) {
					node.mutex.Lock()
					node.votes[targetID] = true
					votes++
					if votes >= len(node.activeNodes)/2+1 && !node.Leader {
						node.Leader = true
						node.lastKnownLeader = node.ID
						updateLastHeartbeat()
						votingComplete <- true
					}
					node.mutex.Unlock()
				}
			}(id)
		}
	}

	<-votingComplete
}

func requestVote(node *Node, targetID, term int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("node-%d:%d", targetID, basePort+targetID), time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()

	msg := Message{
		Type: "VoteRequest",
		VoteRequest: VoteRequest{
			CandidateID: node.ID,
			Term:        term,
		},
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		return false
	}

	var response VoteResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		return false
	}

	return response.VoteGranted
}

func listenForHeartbeats(node *Node) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", basePort+node.ID))
	if err != nil {
		log.Printf("Error starting listener: %v\n", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(node, conn)
	}
}

func handleConnection(node *Node, conn net.Conn) {
	defer conn.Close()

	var msg Message
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&msg); err != nil {
		return
	}

	node.mutex.Lock()
	defer node.mutex.Unlock()

	switch msg.Type {
	case "VoteRequest":
		response := VoteResponse{
			VoteGranted: false,
			Term:        node.term,
		}

		if msg.VoteRequest.Term > node.term ||
			(msg.VoteRequest.Term == node.term &&
				msg.VoteRequest.CandidateID < node.ID) {
			response.VoteGranted = true
			node.term = msg.VoteRequest.Term
			node.Leader = false
			node.lastKnownLeader = msg.VoteRequest.CandidateID
			updateLastHeartbeat()
		}

		encoder := json.NewEncoder(conn)
		encoder.Encode(response)

	case "Heartbeat":
		// Update term and leader if heartbeat is from current or newer term
		if msg.Heartbeat.Term >= node.term {
			node.term = msg.Heartbeat.Term
			node.Leader = false // This node is definitely not the leader
			node.lastKnownLeader = msg.Heartbeat.Leader
			updateLastHeartbeat()
		}
	}
}

func monitorMembershipChanges(node *Node) {
	ticker := time.NewTicker(heartbeatInterval)
	for range ticker.C {
		resp, err := http.Get(fmt.Sprintf("http://%s/members", node.membershipHost))
		if err != nil {
			continue
		}

		var members map[string]*MemberInfo1

		if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
			resp.Body.Close()
			continue
		}

		resp.Body.Close()

		node.mutex.Lock()

		for k := range node.activeNodes {
			delete(node.activeNodes, k)
		}

		for id, member := range members {
			nodeID, _ := strconv.Atoi(id)
			node.activeNodes[nodeID] = true

			if member.IsLeader {
				node.lastKnownLeader = nodeID
			}
		}

		node.mutex.Unlock()
	}
}

func getMembershipList(membershipHost string) (map[string]*MemberInfo1, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/members", membershipHost))
	if err != nil {
		return nil, fmt.Errorf("failed to get members: %v", err)
	}
	defer resp.Body.Close()

	var members map[string]*MemberInfo1
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return nil, fmt.Errorf("failed to decode members: %v", err)
	}
	return members, nil
}

func recognizeLeader(node *Node) {
	node.mutex.Lock()
	defer node.mutex.Unlock()

	activeCount := 0
	for _, active := range node.activeNodes {
		if active {
			activeCount++
		}
	}

	// Calculate quorum size dynamically
	quorumSize := (activeCount / 2) + 1

	if activeCount < quorumSize {
		node.Leader = false
		return
	}

	if node.lastKnownLeader > 0 && node.activeNodes[node.lastKnownLeader] {
		node.Leader = (node.ID == node.lastKnownLeader)
	}
}

func startHTTPServer(node *Node) {
	http.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		node.mutex.RLock()
		defer node.mutex.RUnlock()
		fmt.Fprintf(w, "Current leader: Node %d (Term: %d)\n", node.lastKnownLeader, node.term)
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		node.mutex.RLock()
		defer node.mutex.RUnlock()
		status := struct {
			NodeID        int
			IsLeader      bool
			Term          int
			ActiveNodes   map[int]bool
			CurrentLeader int
		}{
			NodeID:        node.ID,
			IsLeader:      node.Leader,
			Term:          node.term,
			ActiveNodes:   node.activeNodes,
			CurrentLeader: node.lastKnownLeader,
		}
		json.NewEncoder(w).Encode(status)
	})

	http.HandleFunc("/query", handleQuery)

	http.HandleFunc("/recvMulticast", recvMulticast)

	fmt.Printf("Starting HTTP server on port %d\n", httpPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil); err != nil {
		fmt.Printf("Error starting HTTP server: %v\n", err)
	}
}

func discoverExistingLeader(node *Node) bool {
	for i := 1; i <= maxNodes; i++ {
		if pingNode(i) {
			leader, term, err := askForLeader(i)
			if err == nil && leader > 0 {
				node.mutex.Lock()
				if term >= node.term {
					node.term = term
					node.Leader = (node.ID == leader)
					node.lastKnownLeader = leader
					updateLastHeartbeat()
				}
				node.mutex.Unlock()
				fmt.Printf("Node %d: Discovered existing leader: Node %d (Term: %d)\n", node.ID, leader, term)
				return true
			}
		}
	}
	return false
}

func askForLeader(nodeID int) (int, int, error) {
	resp, err := http.Get(fmt.Sprintf("http://node-%d:%d/leader", nodeID, httpPort))
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	var leader, term int
	_, err = fmt.Fscanf(resp.Body, "Current leader: Node %d (Term: %d)", &leader, &term)
	return leader, term, err
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

func pingNode(id int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("node-%d:%d", id, basePort+id), time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func sendHeartbeats(node *Node) {
	node.mutex.RLock()
	if !node.Leader {
		node.mutex.RUnlock()
		return // Early return if not leader
	}
	currentTerm := node.term
	nodeID := node.ID
	node.mutex.RUnlock()

	msg := Message{
		Type: "Heartbeat",
		Heartbeat: struct {
			Term   int
			Leader int
		}{
			Term:   currentTerm,
			Leader: nodeID,
		},
	}

	for i := 1; i <= maxNodes; i++ {
		if i != nodeID {
			go func(targetID int) {
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("node-%d:%d", targetID, basePort+targetID), time.Second)
				if err != nil {
					return
				}
				defer conn.Close()

				encoder := json.NewEncoder(conn)
				if err := encoder.Encode(msg); err != nil {
					return
				}

				fmt.Printf("Node %d: Sent heartbeat to Node %d (Term: %d)\n", nodeID, targetID, currentTerm)
			}(i)
		}
	}
}
