package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type MembershipManager struct {
	members    map[string]*MemberInfo
	mu         sync.RWMutex
	watchChan  chan MembershipEvent
	httpServer *http.Server
}

type MemberInfo struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	LeaseID   int64     `json:"lease_id"`
	ExpiresAt time.Time `json:"expires_at"`
	IsLeader  bool      `json:"is_leader"`
}

type MembershipEvent struct {
	Type    EventType `json:"type"`
	NodeID  string    `json:"node_id"`
	Address string    `json:"address"`
}

type EventType int

const (
	NodeJoined EventType = iota
	NodeLeft
)

func main() {
	mm := NewMembershipManager()
	log.Printf("Membership service started on port 7946")

	// Wait for server shutdown
	if err := mm.httpServer.ListenAndServe(); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

func NewMembershipManager() *MembershipManager {
	mm := &MembershipManager{
		members:   make(map[string]*MemberInfo),
		watchChan: make(chan MembershipEvent, 100),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/members", mm.handleMembers)
	mux.HandleFunc("/register", mm.handleRegister)
	mux.HandleFunc("/keepalive", mm.handleKeepAlive)
	mux.HandleFunc("/leader", mm.handleLeader)

	mm.httpServer = &http.Server{
		Addr:    ":7946",
		Handler: mux,
	}

	go mm.leaseManager()

	return mm
}

func (mm *MembershipManager) leaseManager() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		mm.mu.Lock()
		now := time.Now()
		for id, member := range mm.members {
			if now.After(member.ExpiresAt) {
				delete(mm.members, id)
				mm.watchChan <- MembershipEvent{
					Type:   NodeLeft,
					NodeID: id,
				}
				log.Printf("Node %s lease expired", id)
			}
		}
		mm.mu.Unlock()
	}
}

func (mm *MembershipManager) handleMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mm.mu.RLock()
	defer mm.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(mm.members); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (mm *MembershipManager) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var info MemberInfo
	if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mm.mu.Lock()
	info.LeaseID = time.Now().UnixNano()
	info.ExpiresAt = time.Now().Add(2 * time.Second)
	mm.members[info.ID] = &info

	mm.watchChan <- MembershipEvent{
		Type:    NodeJoined,
		NodeID:  info.ID,
		Address: info.Address,
	}
	mm.mu.Unlock()

	log.Printf("Node %s registered", info.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"lease_id": info.LeaseID})
}

func (mm *MembershipManager) handleKeepAlive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var info MemberInfo
	if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mm.mu.Lock()
	if member, exists := mm.members[info.ID]; exists {
		member.ExpiresAt = time.Now().Add(2 * time.Second)
		member.IsLeader = info.IsLeader
		log.Printf("Node %s keepalive received, leader status: %v", info.ID, info.IsLeader)
	}
	mm.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (mm *MembershipManager) handleLeader(w http.ResponseWriter, r *http.Request) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	for _, member := range mm.members {
		if member.IsLeader {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(member)
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
}
