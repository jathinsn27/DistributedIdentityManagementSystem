package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
)

type Edge struct {
	From, To, Weight int
}

type Graph struct {
	Nodes int
	Edges []Edge
}

type NeighborMessage struct {
	Neighbors []int `json:"neighbors"`
}

// Minimum Spanning Tree (Kruskal's Algorithm)
func (g *Graph) KruskalMST() []Edge {
	sort.Slice(g.Edges, func(i, j int) bool {
		return g.Edges[i].Weight < g.Edges[j].Weight
	})

	parent := make([]int, g.Nodes)
	rank := make([]int, g.Nodes)
	for i := range parent {
		parent[i] = i
	}

	find := func(node int) int {
		if parent[node] != node {
			parent[node] = find(parent[node])
		}
		return parent[node]
	}

	union := func(x, y int) {
		rootX := find(x)
		rootY := find(y)
		if rootX != rootY {
			if rank[rootX] > rank[rootY] {
				parent[rootY] = rootX
			} else if rank[rootX] < rank[rootY] {
				parent[rootX] = rootY
			} else {
				parent[rootY] = rootX
				rank[rootX]++
			}
		}
	}

	mst := []Edge{}
	for _, edge := range g.Edges {
		if find(edge.From) != find(edge.To) {
			mst = append(mst, edge)
			union(edge.From, edge.To)
		}
		if len(mst) == g.Nodes-1 {
			break
		}
	}
	return mst
}

func main() {
	// Example Graph
	graph := Graph{
		Nodes: 4,
		Edges: []Edge{
			{From: 0, To: 1, Weight: 1},
			{From: 0, To: 2, Weight: 4},
			{From: 1, To: 2, Weight: 2},
			{From: 1, To: 3, Weight: 3},
			{From: 2, To: 3, Weight: 5},
		},
	}

	// Calculate MST
	mst := graph.KruskalMST()
	fmt.Println("MST Computed:", mst)

	// Build adjacency list
	adjList := make(map[int][]int)
	for _, edge := range mst {
		adjList[edge.From] = append(adjList[edge.From], edge.To)
		adjList[edge.To] = append(adjList[edge.To], edge.From)
	}

	// Notify each container about its neighbors
	for node, neighbors := range adjList {
		message := NeighborMessage{Neighbors: neighbors}
		jsonData, err := json.Marshal(message)
		if err != nil {
			fmt.Println("Error marshalling JSON:", err)
			continue
		}

		url := fmt.Sprintf("http://node%d:8080/notify", node)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error notifying node %d: %v\n", node, err)
			continue
		}
		fmt.Printf("Notified node %d: %v\n", node, resp.Status)
	}
}
