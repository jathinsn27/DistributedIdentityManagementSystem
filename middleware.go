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
	queueCapacity  = 1000
)

type Request struct {
	w    http.ResponseWriter
	r    *http.Request
	done chan error
}

type Middleware struct {
	currentLeader int
	mutex         sync.RWMutex
	requestQueue  chan Request
	isLeaderUp    bool
}

func NewMiddleware() *Middleware {
	m := &Middleware{
		requestQueue: make(chan Request, queueCapacity),
		isLeaderUp:   false,
	}
	go m.pollForLeader()
	go m.processQueue()
	return m
}

func (m *Middleware) pollForLeader() {
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		leaderFound := false
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
				m.isLeaderUp = true
				leaderFound = true
				m.mutex.Unlock()
				break
			}
		}

		if !leaderFound {
			m.mutex.Lock()
			m.isLeaderUp = false
			log.Printf("No leader available, requests will be queued")
			m.mutex.Unlock()
		}

		time.Sleep(pollInterval)
	}
}

func (m *Middleware) processQueue() {
	for req := range m.requestQueue {
		m.mutex.RLock()
		leader := m.currentLeader
		isLeaderUp := m.isLeaderUp
		m.mutex.RUnlock()

		if !isLeaderUp {
			// Keep request in queue by re-queuing it
			select {
			case m.requestQueue <- req:
				log.Printf("Re-queued request, waiting for leader")
			default:
				req.done <- fmt.Errorf("queue full")
			}
			time.Sleep(1 * time.Second)
			continue
		}

		// Forward request to leader
		targetURL, _ := url.Parse(fmt.Sprintf("http://node-%d:%d", leader, nodeBasePort))
		proxy := httputil.NewSingleHostReverseProxy(targetURL)

		ctx, cancel := context.WithTimeout(req.r.Context(), 5*time.Second)
		defer cancel()

		err := m.forwardRequest(proxy, req.w, req.r.WithContext(ctx))
		req.done <- err
	}
}

func (m *Middleware) forwardRequest(proxy *httputil.ReverseProxy, w http.ResponseWriter, r *http.Request) error {
	errChan := make(chan error, 1)
	go func() {
		proxy.ServeHTTP(w, r)
		errChan <- nil
	}()

	select {
	case err := <-errChan:
		return err
	case <-r.Context().Done():
		return r.Context().Err()
	}
}

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	done := make(chan error, 1)
	req := Request{
		w:    w,
		r:    r,
		done: done,
	}

	// Try to queue the request
	select {
	case m.requestQueue <- req:
		if err := <-done; err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
	case <-time.After(10 * time.Second):
		http.Error(w, "Request timeout - queue full", http.StatusServiceUnavailable)
	}
}

func main() {
	middleware := NewMiddleware()
	log.Printf("Starting middleware on port %d", middlewarePort)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", middlewarePort),
		Handler:      middleware,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	log.Fatal(server.ListenAndServe())
}

// package main

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"net/http/httputil"
// 	"net/url"
// 	"sync"
// 	"time"
// )

// const (
// 	middlewarePort = 8090
// 	nodeBasePort   = 8080
// 	max_Nodes      = 4
// 	pollInterval   = 5 * time.Second
// )

// type Middleware struct {
// 	currentLeader int
// 	mutex         sync.RWMutex
// }

// func NewMiddleware() *Middleware {
// 	m := &Middleware{}
// 	go m.pollForLeader()
// 	return m
// }

// func (m *Middleware) pollForLeader() {
// 	client := &http.Client{Timeout: 2 * time.Second}
// 	for {
// 		for i := 1; i <= max_Nodes; i++ {
// 			resp, err := client.Get(fmt.Sprintf("http://node-%d:%d/leader", i, nodeBasePort))
// 			if err != nil {
// 				continue
// 			}
// 			defer resp.Body.Close()

// 			var leader int
// 			_, err = fmt.Fscanf(resp.Body, "Current leader: Node %d", &leader)
// 			if err == nil && leader > 0 && leader <= max_Nodes {
// 				m.mutex.Lock()
// 				if m.currentLeader != leader {
// 					log.Printf("New leader detected: Node %d", leader)
// 				}
// 				m.currentLeader = leader
// 				m.mutex.Unlock()
// 				break
// 			}
// 		}
// 		time.Sleep(pollInterval)
// 	}
// }

// func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	m.mutex.RLock()
// 	leader := m.currentLeader
// 	m.mutex.RUnlock()

// 	if leader == 0 {
// 		http.Error(w, "No leader available", http.StatusServiceUnavailable)
// 		return
// 	}

// 	targetURL, _ := url.Parse(fmt.Sprintf("http://node-%d:%d", leader, nodeBasePort))
// 	proxy := httputil.NewSingleHostReverseProxy(targetURL)

// 	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
// 	defer cancel()

// 	proxy.ServeHTTP(w, r.WithContext(ctx))
// }

// func main() {
// 	middleware := NewMiddleware()
// 	log.Printf("Starting middleware on port %d", middlewarePort)

// 	server := &http.Server{
// 		Addr:         fmt.Sprintf(":%d", middlewarePort),
// 		Handler:      middleware,
// 		ReadTimeout:  10 * time.Second,
// 		WriteTimeout: 10 * time.Second,
// 	}

// 	log.Fatal(server.ListenAndServe())
// }
