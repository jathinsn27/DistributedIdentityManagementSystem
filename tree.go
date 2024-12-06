package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"sync"
)

func GetTree() *SpanningTree {
	treeOnce.Do(func() {
		globalTree = &SpanningTree{
			Root: nil,
			mu:   sync.RWMutex{},
		}
	})
	return globalTree
}

type GetTreeRequest struct {
	Leader  string `json:"leader"`
	Address string `json:"address"`
	Node    string `json:"recoveredNode"`
	Naddr   string `json:"naddr"`
}

type SerializableNode struct {
	ID       string             `json:"id"`
	Address  string             `json:"address"`
	ParentID string             `json:"parent_id,omitempty"`
	Children []SerializableNode `json:"children,omitempty"`
}

func (s *SpanningTree) AddNode(nodeID, address, leaderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node := &SpanningTreeNode{
		ID:       nodeID,
		address:  address,
		Parent:   nil,
		Children: make([]*SpanningTreeNode, 0),
		mu:       sync.RWMutex{},
	}

	if s.Root.FindNodeDFS(nodeID) != nil {
		return
	}

	if s.Root == nil {
		// If there's no root, this node becomes the root
		s.Root = node
	} else {
		// Use BFS or another method to find where to add this node
		queue := []*SpanningTreeNode{s.Root}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]

			if len(current.Children) < 2 {
				current.Children = append(current.Children, node)
				node.Parent = current
				break
			}

			// Add children to queue for further exploration
			queue = append(queue, current.Children...)
		}
	}
	fmt.Println("the rebalancing is starting")
	// Rebalance from the root downwards
	s.Root = rebalance(s.Root)

	fmt.Println("The rebalancing is done")
	// Ensure leader remains root
	if s.Root.ID != leaderID {
		s.EnsureLeaderAsRoot(leaderID)
	}
}

func (s *SpanningTree) RemoveNode(nodeID string, leaderID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Root == nil {
		return
	}

	var removeNode func(*SpanningTreeNode, string) *SpanningTreeNode
	removeNode = func(node *SpanningTreeNode, id string) *SpanningTreeNode {
		if node == nil {
			return nil
		}

		if id < node.ID {
			if len(node.Children) > 0 {
				node.Children[0] = removeNode(node.Children[0], id)
			}
		} else if id > node.ID {
			if len(node.Children) > 1 {
				node.Children[1] = removeNode(node.Children[1], id)
			}
		} else {
			// Node found: handle removal logic here

			if len(node.Children) == 0 {
				return nil
			} else if len(node.Children) == 1 {
				return node.Children[0]
			} else {
				successor := findMin(node.Children[1])
				if successor == nil || successor.ID == node.ID {
					return node.Children[0]
				}
				node.ID = successor.ID
				node.address = successor.address
				node.Children[1] = removeNode(node.Children[1], successor.ID)
			}
		}

		return rebalance(node)
	}

	s.Root = removeNode(s.Root, nodeID)

	// Ensure leader remains root after removal and rebalancing.
	s.EnsureLeaderAsRoot(leaderID)
}

// Helper function to find minimum value in a subtree (in-order successor)
func findMin(node *SpanningTreeNode) *SpanningTreeNode {
	current := node
	if current == nil {
		return nil
	}
	for len(current.Children) > 0 && current.Children[0] != nil {
		current = current.Children[0]
	}
	return current
}

func (s *SpanningTree) EnsureLeaderAsRoot(leaderID string) {
	fmt.Printf("start swapping")
	if s.Root != nil && s.Root.ID != leaderID {
		leaderNode := s.Root.FindNodeDFS(leaderID)
		fmt.Printf("Insisde ensureleader %v", leaderNode)
		if leaderNode != nil {
			// Swap IDs and addresses between the root and the leader node
			s.Root.ID, leaderNode.ID = leaderNode.ID, s.Root.ID
			s.Root.address, leaderNode.address = leaderNode.address, s.Root.address
		}
	}
}

func rebalance(node *SpanningTreeNode) *SpanningTreeNode {
	if node == nil {
		return nil
	}

	leftHeight := 0
	rightHeight := 0

	if len(node.Children) > 0 {
		leftHeight = height(node.Children[0])
	}
	if len(node.Children) > 1 {
		rightHeight = height(node.Children[1])
	}

	balance := leftHeight - rightHeight

	// Left heavy
	if balance > 1 {
		if len(node.Children[0].Children) > 1 && height(node.Children[0].Children[0]) >= height(node.Children[0].Children[1]) {
			// Left-Left case
			return rotateRight(node)
		} else {
			// Left-Right case
			node.Children[0] = rotateLeft(node.Children[0])
			return rotateRight(node)
		}
	}

	// Right heavy
	if balance < -1 {
		if len(node.Children[1].Children) > 1 && height(node.Children[1].Children[1]) >= height(node.Children[1].Children[0]) {
			// Right-Right case
			return rotateLeft(node)
		} else {
			// Right-Left case
			node.Children[1] = rotateRight(node.Children[1])
			return rotateLeft(node)
		}
	}

	return node
}

// Helper function to perform a right rotation
func rotateRight(y *SpanningTreeNode) *SpanningTreeNode {
	if y == nil || len(y.Children) == 0 {
		return y
	}

	x := y.Children[0]
	if x == nil {
		return y
	}

	// Safely handle T2 (which might not exist)
	var T2 *SpanningTreeNode
	if len(x.Children) > 1 {
		T2 = x.Children[1]
	}

	// Ensure x has enough capacity for children
	if len(x.Children) < 2 {
		x.Children = append(x.Children, nil)
	}

	// Perform rotation
	x.Children[1] = y

	// Ensure y has enough capacity for children
	if len(y.Children) < 1 {
		y.Children = append(y.Children, nil)
	}
	y.Children[0] = T2

	return x
}

// Helper function to perform a left rotation
func rotateLeft(x *SpanningTreeNode) *SpanningTreeNode {
	// Check if x exists and has enough children
	if x == nil || len(x.Children) < 2 {
		return x
	}

	// Check if right child exists
	y := x.Children[1]
	if y == nil {
		return x
	}

	// Safely handle T2 (which might not exist)
	var T2 *SpanningTreeNode
	if len(y.Children) > 0 {
		T2 = y.Children[0]
	}

	// Ensure y has enough capacity for children
	if len(y.Children) < 1 {
		y.Children = append(y.Children, nil)
	}

	// Perform rotation
	y.Children[0] = x

	// Ensure x has enough capacity for children
	if len(x.Children) < 2 {
		x.Children = append(x.Children, nil)
	}
	x.Children[1] = T2

	return y
}

// Helper function to calculate the height of a node
func height(n *SpanningTreeNode) int {
	if n == nil {
		return 0
	}

	leftHeight := 0
	rightHeight := 0

	if len(n.Children) > 0 {
		leftHeight = height(n.Children[0])
	}

	if len(n.Children) > 1 {
		rightHeight = height(n.Children[1])
	}

	if leftHeight > rightHeight {
		return leftHeight + 1
	}

	return rightHeight + 1
}

func (s *SpanningTree) findParent() *SpanningTreeNode {
	var findNodeWithFewestChildren func(*SpanningTreeNode) *SpanningTreeNode
	findNodeWithFewestChildren = func(n *SpanningTreeNode) *SpanningTreeNode {
		if len(n.Children) < 2 {
			return n
		}
		for _, child := range n.Children {
			if node := findNodeWithFewestChildren(child); node != nil {
				return node
			}
		}
		return nil
	}

	return findNodeWithFewestChildren(s.Root)
}

func GetLeaderTree(leader string, address string, id string, addr string) error {
	getTreeReq := &GetTreeRequest{
		Leader:  leader,
		Address: address,
		Node:    id,
		Naddr:   addr,
	}
	jsonData, err := json.Marshal(getTreeReq)
	if err != nil {
		return fmt.Errorf("failed to marshal get Tree message: %v", err)
	}

	fmt.Printf("http://%s/getTreeFromLeader", address)
	resp, err := http.Post(fmt.Sprintf("http://%s/getTreeFromLeader", address), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send multicast: %v", err)
	}
	defer resp.Body.Close()
	tree := GetGlobalTree()
	body, err := ioutil.ReadAll(resp.Body)
	stree, err := ReconstructTree(body)
	tree.Root = stree.Root
	return nil
}

func GetTreeFromLeader(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Received Request for Tree\n")
	var req GetTreeRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		fmt.Printf("Error decoding GetTree request: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	tree := GetGlobalTree()
	tree.AddNode(req.Node, req.Naddr, req.Leader)
	serialTree, err := tree.Root.ToSerializable()
	w.Header().Set("Content-Type", "application/json")
	prevMembershipList = append(prevMembershipList, req.Node)
	json.NewEncoder(w).Encode(serialTree)
}

func ConstructSpanningTree(tree *SpanningTree, members map[string]*MemberInfo1, leader string) error {
	fmt.Printf("Recovery : %v\n", recovery)
	if recovery == true {
		id := fmt.Sprint(os.Getenv("NODE_ID"))
		return GetLeaderTree(leader, members[leader].Address, id, members[id].Address)
	}
	keys := make([]string, 0, len(members))
	for k := range members {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Printf("Keys : %v", keys)

	for _, key := range keys {
		memberInfo := members[key]
		fmt.Println("going to add the node")
		tree.AddNode(key, memberInfo.Address, leader) // Assuming Address is a string field in MemberInfo1
	}
	fmt.Printf("Tree.root %s :%s\n", tree.Root.ID, tree.Root.address)
	return nil
}

func (node *SpanningTreeNode) FindNodeDFS(targetID string) *SpanningTreeNode {
	if node == nil || targetID == "" {
		return nil
	}

	// Initialize stack and visited map
	stack := []*SpanningTreeNode{node}
	visited := make(map[string]bool)

	for len(stack) > 0 {
		// Pop the top node from stack
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Skip if node is nil or already visited
		if current == nil {
			continue
		}

		current.mu.RLock()
		nodeID := current.ID

		// Check if already visited
		if visited[nodeID] {
			current.mu.RUnlock()
			continue
		}

		// Mark as visited
		visited[nodeID] = true

		// Check if this is the target node
		if nodeID == targetID {
			current.mu.RUnlock()
			return current
		}

		// Add unvisited children to stack
		for i := len(current.Children) - 1; i >= 0; i-- {
			if child := current.Children[i]; child != nil && !visited[child.ID] {
				stack = append(stack, child)
			}
		}
		current.mu.RUnlock()
	}
	return nil
}

func (st *SpanningTree) PrintTree() {
	if st.Root == nil {
		fmt.Println("Empty tree")
		return
	}

	fmt.Println("Spanning Tree Structure:")
	fmt.Println("=======================")
	st.printDetailedNode(st.Root, "", true)
}

func (st *SpanningTree) printDetailedNode(node *SpanningTreeNode, prefix string, isLast bool) {
	if node == nil {
		return
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	// Choose the appropriate branch symbol
	branch := "├──"
	if isLast {
		branch = "└──"
	}

	// Print current node with branch
	fmt.Printf("%s%s Node[ID: %s]\n", prefix, branch, node.ID)
	fmt.Printf("%s    Address: %s\n", prefix, node.address)

	// Prepare prefix for children
	childPrefix := prefix
	if isLast {
		childPrefix += "    "
	} else {
		childPrefix += "│   "
	}

	// Print children
	for i, child := range node.Children {
		isLastChild := i == len(node.Children)-1
		st.printDetailedNode(child, childPrefix, isLastChild)
	}
}

func (node *SpanningTreeNode) ToSerializable() (SerializableNode, error) {
	if node == nil {
		return SerializableNode{}, nil
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	serialNode := SerializableNode{
		ID:      node.ID,
		Address: node.address,
	}

	if node.Parent != nil {
		serialNode.ParentID = node.Parent.ID
	}

	// Only serialize non-nil children
	for _, child := range node.Children {
		if child == nil {
			continue
		}
		serializedChild, err := child.ToSerializable()
		if err != nil {
			return SerializableNode{}, fmt.Errorf("error serializing child node: %v", err)
		}
		serialNode.Children = append(serialNode.Children, serializedChild)
	}

	return serialNode, nil
}

// Function to reconstruct the tree from JSON
func ReconstructTree(data []byte) (*SpanningTree, error) {
	var serialNode SerializableNode
	if err := json.Unmarshal(data, &serialNode); err != nil {
		return nil, err
	}

	nodeMap := make(map[string]*SpanningTreeNode)
	root := reconstructNode(&serialNode, nil, nodeMap)

	return &SpanningTree{Root: root}, nil
}

func reconstructNode(serialNode *SerializableNode, parent *SpanningTreeNode, nodeMap map[string]*SpanningTreeNode) *SpanningTreeNode {
	node := &SpanningTreeNode{
		ID:       serialNode.ID,
		address:  serialNode.Address,
		Parent:   parent,
		Children: make([]*SpanningTreeNode, 0),
	}

	nodeMap[node.ID] = node

	for _, child := range serialNode.Children {
		childNode := reconstructNode(&child, node, nodeMap)
		node.Children = append(node.Children, childNode)
	}

	return node
}
