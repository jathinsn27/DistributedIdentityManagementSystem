package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

const (
	middlewarePort = 8090
	nodeBasePort   = 8080
	max_Nodes      = 4
	pollInterval   = 5 * time.Second
)

type Middleware struct {
	currentLeader int
	mutex         sync.RWMutex
}

func NewMiddleware() *Middleware {
	m := &Middleware{}
	go m.pollForLeader()
	return m
}

func (m *Middleware) pollForLeader() {
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		for i := 1; i <= max_Nodes; i++ {
			resp, err := client.Get(fmt.Sprintf("http://node-%d:%d/leader", i, nodeBasePort))
			if err != nil {
				continue
			}
			defer resp.Body.Close()

			var leader int
			_, err = fmt.Fscanf(resp.Body, "Current leader: Node %d", &leader)
			if err == nil && leader > 0 && leader <= max_Nodes {
				m.mutex.Lock()
				if m.currentLeader != leader {
					log.Printf("New leader detected: Node %d", leader)
				}
				m.currentLeader = leader
				m.mutex.Unlock()
				break
			}
		}
		time.Sleep(pollInterval)
	}
}

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mutex.RLock()
	leader := m.currentLeader
	m.mutex.RUnlock()

	if leader == 0 {
		http.Error(w, "No leader available", http.StatusServiceUnavailable)
		return
	}

	targetURL, _ := url.Parse(fmt.Sprintf("http://node-%d:%d", leader, nodeBasePort))
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	proxy.ServeHTTP(w, r.WithContext(ctx))
}

func main() {
	middleware := NewMiddleware()
	log.Printf("Starting middleware on port %d", middlewarePort)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", middlewarePort),
		Handler:      middleware,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Fatal(server.ListenAndServe())
}
