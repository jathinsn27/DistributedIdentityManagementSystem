package main

import (
	"fmt"
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
		if len(node.Children[0].Children) > 0 && height(node.Children[0].Children[0]) >= height(node.Children[0].Children[1]) {
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

func ConstructSpanningTree(tree *SpanningTree, members map[string]*MemberInfo1, leader string) error {
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

func GetLeaderId(members map[string]*MemberInfo1) (string, error) {
	for _, memberInfo := range members {
		if memberInfo.IsLeader {
			return memberInfo.ID, nil
		}
	}
	return "", fmt.Errorf("leader not found")
}

func (node *SpanningTreeNode) FindNodeDFS(targetID string) *SpanningTreeNode {
	if node == nil {
		return nil
	}

	stack := []*SpanningTreeNode{node}

	for len(stack) > 0 {
		// Pop the top node from stack
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Check if this is the target node
		if current.ID == targetID {
			return current
		}

		// Add children to stack
		current.mu.RLock()
		for i := 0; i < len(current.Children); i++ {
			stack = append(stack, current.Children[i])
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
