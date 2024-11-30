package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

    "DistributedIdentityManagementSystem/node"
    "DistributedIdentityManagementSystem/server"
    pb "DistributedIdentityManagementSystem/proto"
)

const (
	basePort          = 8000
	maxNodes          = 4
	httpPort          = 8080
	heartbeatInterval = 2 * time.Second
	leaderTimeout     = 4 * time.Second
	quorumSize        = (maxNodes / 2) + 1
	etcdPrefix        = "/members/"
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
    Parent          *Node
	Children        []*Node
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

var (
	lastHeartbeat  time.Time
	heartbeatMutex sync.RWMutex
)

func main() {
	nodeID, _ := strconv.Atoi(os.Getenv("NODE_ID"))
	node := &Node{
		ID:          nodeID,
		activeNodes: make(map[int]bool),
		votes:       make(map[int]bool),
		term:        0,
		address:     os.Getenv("NODE_MCAST_ADDR"),
	}

	err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Connect to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// Join the cluster
	if err := joinCluster(cli, node); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s joined the cluster\n", nodeID)

	// Watch for membership changes
	go listenForHeartbeats(node)
	go startHTTPServer(node)
	go monitorClusterHealth(node)

	time.Sleep(5 * time.Second) // Wait for all nodes to start
	go watchMembership(cli)

	if !discoverExistingLeader(node) {
		startElection(node)
	}

	for {

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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		lease, err := cli.Grant(context.Background(), 2)

		if err != nil {
			log.Printf("Error keeping node alive: %v", err)
		}
		if _, err := cli.Put(ctx, etcdPrefix+strconv.Itoa(node.ID), node.address, clientv3.WithLease(lease.ID)); err != nil {
			log.Printf("Error keeping node alive: %v", err)
		}

		time.Sleep(heartbeatInterval)
	}
}

func joinCluster(cli *clientv3.Client, node *Node) error {
	lease, err := cli.Grant(context.Background(), 2)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Put(ctx, etcdPrefix+strconv.Itoa(node.ID), node.address, clientv3.WithLease(lease.ID))
	if err != nil {
		return err
	}

	return nil
}

func watchMembership(cli *clientv3.Client) {
	watcher := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
	for response := range watcher {
		for _, ev := range response.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				nodeID := string(ev.Kv.Key[len(etcdPrefix):])
				address := string(ev.Kv.Value)
				tree.AddNode(nodeID, address)
				log.Printf("Node added: %s at %s", nodeID, address)
			case clientv3.EventTypeDelete:
				nodeID := string(ev.Kv.Key[len(etcdPrefix):])
				tree.RemoveNode(nodeID)
				log.Printf("Node removed: %s", nodeID)
			}
		}

		// Query etcd to get the current member list
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := cli.Get(ctx, etcdPrefix, clientv3.WithPrefix())
		if err != nil {
			log.Printf("Failed to get member list from etcd: %v", err)
			continue
		}

		members := []string{}

		for _, kv := range resp.Kvs {
			members = append(members, fmt.Sprintf("%s: %s", kv.Key[len(etcdPrefix):], kv.Value))
		}
		fmt.Printf("Current Member list: %s\n", strings.Join(members, ","))
	}
}

func startElection(node *Node) {
	time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)

	node.mutex.Lock()
	// Don't start election if we already have a leader
	if node.lastKnownLeader > 0 && node.activeNodes[node.lastKnownLeader] {
		node.mutex.Unlock()
		return
	}

	// Check if enough time has passed since last heartbeat
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
	node.votes[node.ID] = true // Vote for self
	node.mutex.Unlock()

	votes := 1
	votingComplete := make(chan bool)

	// Set timeout for election
	go func() {
		time.Sleep(2 * time.Second)
		votingComplete <- true
	}()

	// Collect votes
	for i := 1; i <= maxNodes; i++ {
		if i != node.ID {
			go func(targetID int) {
				if requestVote(node, targetID, currentTerm) {
					node.mutex.Lock()
					node.votes[targetID] = true
					votes++
					if votes >= quorumSize && !node.Leader {
						node.Leader = true
						node.lastKnownLeader = node.ID
						updateLastHeartbeat()
						votingComplete <- true
					}
					node.mutex.Unlock()
				}
			}(i)
		}
	}

	<-votingComplete // Wait for election to complete
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

func monitorClusterHealth(node *Node) {
	for {
		node.mutex.Lock()
		node.activeNodes = make(map[int]bool)
		for i := 1; i <= maxNodes; i++ {
			node.activeNodes[i] = pingNode(i)
		}
		node.mutex.Unlock()
		time.Sleep(heartbeatInterval)
	}
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