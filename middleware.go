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
	maxNodesmid    = 10
	pollInterval   = 5 * time.Second
	checkTimeout   = 2 * time.Second
)

type Middleware struct {
	currentLeader int
	mutex         sync.RWMutex
}

type customResponseWriter struct {
	http.ResponseWriter
	headerWritten bool
	statusCode    int
}

func (crw *customResponseWriter) WriteHeader(statusCode int) {
	if !crw.headerWritten {
		crw.ResponseWriter.WriteHeader(statusCode)
		crw.headerWritten = true
		crw.statusCode = statusCode
	}
}

func (crw *customResponseWriter) Write(b []byte) (int, error) {
	if !crw.headerWritten {
		crw.WriteHeader(http.StatusOK)
	}
	return crw.ResponseWriter.Write(b)
}

func NewMiddleware() *Middleware {
	m := &Middleware{}
	go m.pollForLeader()
	return m
}

func (m *Middleware) pollForLeader() {
	client := &http.Client{Timeout: checkTimeout}
	for {
		for i := 1; i <= maxNodesmid; i++ {
			resp, err := client.Get(fmt.Sprintf("http://node-%d:%d/leader", i, nodeBasePort))
			if err != nil {
				continue
			}
			defer resp.Body.Close()

			var leader int
			_, err = fmt.Fscanf(resp.Body, "Current leader: Node %d", &leader)
			if err == nil && leader > 0 && leader <= maxNodesmid {
				m.mutex.Lock()
				if m.currentLeader != leader {
					log.Printf("New leader detected: Node %d\n", leader)
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

	crw := &customResponseWriter{ResponseWriter: w}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	done := make(chan bool, 1)

	go func() {
		proxy.ServeHTTP(crw, r.WithContext(ctx))
		done <- true
	}()

	select {
	case <-done:
		// Request completed successfully
	case <-ctx.Done():
		m.mutex.Lock()
		m.currentLeader = 0
		m.mutex.Unlock()
		if !crw.headerWritten {
			http.Error(crw, "Request timeout, no leader available", http.StatusGatewayTimeout)
		}
	}
}

func main() {
	middleware := NewMiddleware()
	log.Printf("Starting middleware on port %d\n", middlewarePort)
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", middlewarePort),
		Handler:      middleware,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}
