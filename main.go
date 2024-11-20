// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// LamportTimestamp represents the logical clock
type LamportTimestamp struct {
	Counter int
	NodeID  int
}

// IAMEvent represents an event in the system
type IAMEvent struct {
	EventID     string                 `bson:"event_id"`
	EventType   string                 `bson:"event_type"`
	Data        map[string]interface{} `bson:"data"`
	Timestamp   int                    `bson:"timestamp"`
	NodeID      int                    `bson:"node_id"`
	VectorClock map[int]int            `bson:"vector_clock"`
	CreatedAt   time.Time              `bson:"created_at"`
}

// IAMServer represents a server node in the system
type IAMServer struct {
	NodeID           int
	LamportClock     *LamportTimestamp
	VectorClock      map[int]int
	MongoClient      *mongo.Client
	EventsCollection *mongo.Collection
}

// NewLamportTimestamp creates a new Lamport timestamp
func NewLamportTimestamp(nodeID int) *LamportTimestamp {
	return &LamportTimestamp{
		Counter: 0,
		NodeID:  nodeID,
	}
}

// GetTimestamp increments and returns the current timestamp
func (lt *LamportTimestamp) GetTimestamp() int {
	lt.Counter++
	return lt.Counter
}

// Update updates the timestamp based on received timestamp
func (lt *LamportTimestamp) Update(receivedTimestamp int) int {
	lt.Counter = max(lt.Counter, receivedTimestamp) + 1
	return lt.Counter
}

// NewIAMServer creates a new IAM server
func NewIAMServer(nodeID int, mongoURL string) (*IAMServer, error) {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, err
	}

	return &IAMServer{
		NodeID:           nodeID,
		LamportClock:     NewLamportTimestamp(nodeID),
		VectorClock:      map[int]int{1: 0, 2: 0, 3: 0}, // Initialize for 3 nodes
		MongoClient:      client,
		EventsCollection: client.Database("iam_db").Collection("events"),
	}, nil
}

// CreateEvent creates a new event in the system
func (s *IAMServer) CreateEvent(ctx context.Context, eventType string, data map[string]interface{}) (*IAMEvent, error) {
	// Update local vector clock
	s.VectorClock[s.NodeID]++

	event := &IAMEvent{
		EventID:     uuid.New().String(),
		EventType:   eventType,
		Data:        data,
		Timestamp:   s.LamportClock.GetTimestamp(),
		NodeID:      s.NodeID,
		VectorClock: copyMap(s.VectorClock),
		CreatedAt:   time.Now(),
	}

	// Store in MongoDB
	_, err := s.EventsCollection.InsertOne(ctx, event)
	if err != nil {
		return nil, err
	}

	return event, nil
}

// ReceiveEvent processes a received event from another server
func (s *IAMServer) ReceiveEvent(ctx context.Context, event *IAMEvent) error {
	// Update Lamport clock
	s.LamportClock.Update(event.Timestamp)

	// Update vector clock
	for nodeID, count := range event.VectorClock {
		s.VectorClock[nodeID] = max(s.VectorClock[nodeID], count)
	}

	// Store received event
	_, err := s.EventsCollection.InsertOne(ctx, event)
	return err
}

// CheckCausality determines if events are causal or concurrent
func (s *IAMServer) CheckCausality(event1, event2 *IAMEvent) string {
	happensBefore := false
	happensAfter := false

	for nodeID := range event1.VectorClock {
		if event1.VectorClock[nodeID] > event2.VectorClock[nodeID] {
			happensAfter = true
		} else if event1.VectorClock[nodeID] < event2.VectorClock[nodeID] {
			happensBefore = true
		}
	}

	if happensBefore && happensAfter {
		return "concurrent"
	}
	return "causal"
}

// GetAllEvents retrieves all events from the database
func (s *IAMServer) GetAllEvents(ctx context.Context) ([]*IAMEvent, error) {
	cursor, err := s.EventsCollection.Find(ctx, map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var events []*IAMEvent
	if err = cursor.All(ctx, &events); err != nil {
		return nil, err
	}

	return events, nil
}

// Utility functions
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func copyMap(m map[int]int) map[int]int {
	result := make(map[int]int)
	for k, v := range m {
		result[k] = v
	}
	return result
}

func main() {
	ctx := context.Background()

	// Initialize servers
	servers := make(map[int]*IAMServer)
	mongoURLs := map[int]string{
		1: "mongodb://localhost:27017",
		2: "mongodb://localhost:27018",
		3: "mongodb://localhost:27019",
	}

	for nodeID, url := range mongoURLs {
		server, err := NewIAMServer(nodeID, url)
		if err != nil {
			log.Fatalf("Failed to create server %d: %v", nodeID, err)
		}
		servers[nodeID] = server
		defer server.MongoClient.Disconnect(ctx)
	}

	// Create test events
	event1Data := map[string]interface{}{
		"username": "alice",
		"email":    "alice@example.com",
	}
	event1, err := servers[1].CreateEvent(ctx, "USER_CREATED", event1Data)
	if err != nil {
		log.Fatal(err)
	}

	// Propagate event to other servers
	for nodeID, server := range servers {
		if nodeID != 1 {
			if err := server.ReceiveEvent(ctx, event1); err != nil {
				log.Printf("Failed to propagate event to server %d: %v", nodeID, err)
			}
		}
	}

	// Create another event
	event2Data := map[string]interface{}{
		"username": "alice",
		"status":   "success",
	}
	event2, err := servers[2].CreateEvent(ctx, "USER_AUTHENTICATED", event2Data)
	if err != nil {
		log.Fatal(err)
	}

	// Check causality
	causality := servers[1].CheckCausality(event1, event2)
	fmt.Printf("Events relationship: %s\n", causality)

	// Print all events from each server
	for nodeID, server := range servers {
		fmt.Printf("\nServer %d events:\n", nodeID)
		events, err := server.GetAllEvents(ctx)
		if err != nil {
			log.Printf("Failed to get events from server %d: %v", nodeID, err)
			continue
		}

		for _, event := range events {
			fmt.Printf("Event: %s, Timestamp: %d, Vector Clock: %v\n",
				event.EventType, event.Timestamp, event.VectorClock)
		}
	}
}
