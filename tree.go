package main

import (
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

func (s *SpanningTree) AddNode(nodeID, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node := &SpanningTreeNode{
		ID:      nodeID,           // Replace with actual node ID
		address: address, // Replace with actual address
		Parent:  nil,               // Initially, the node has no parent
		Children:   make([]*SpanningTreeNode, 0), // Initialize an empty slice for children
		mu:      sync.RWMutex{},    // Initialize the mutex
	}

	if s.Root == nil {
		s.Root = node
		return
	}

	parent := s.findParent()
	if (parent != nil) {
		parent.Children = append(parent.Children, node)
	}
	node.Parent = parent
}

func (s *SpanningTree) RemoveNode(nodeID string) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.Root.ID == nodeID {
        if len(s.Root.Children) > 0 {
            newRoot := s.Root.Children[0]
            newRoot.Parent = nil
            s.Root = newRoot
            s.Root.Children = append(s.Root.Children, s.Root.Children[1:]...)
        } else {
            s.Root = nil
        }
        return
    }

    var removeNodeRecursive func(*Node) bool
    removeNodeRecursive = func(n *Node) bool {
        for i, child := range n.Children {
            if child.ID == nodeID {
                n.Children = append(n.Children[:i], n.Children[i+1:]...)
                return true
            }
            if removeNodeRecursive(child) {
                return true
            }
        }
        return false
    }

    removeNodeRecursive(s.Root)
}

func (s *SpanningTree) findParent() *Node {
	var findNodeWithFewestChildren func(*Node) *Node
	findNodeWithFewestChildren = func(n *Node) *Node {
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

func ConstructSpanningTree(tree *SpanningTree, membershipHost string) error {
    resp, err := http.Get(fmt.Sprintf("http://%s/members", membershipHost))
    if err != nil {
        return fmt.Errorf("failed to get members: %v", err)
    }
    defer resp.Body.Close()

    var members map[string]*MemberInfo1
    if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
        return fmt.Errorf("failed to decode members: %v", err)
    }

    keys := make([]string, 0, len(members))
    for k := range members {
        keys = append(keys, k)
    }
    sort.Strings(keys)

    for _, key := range keys {
        memberInfo := members[key]
        tree.AddNode(key, memberInfo.Address) // Assuming Address is a string field in MemberInfo1
    }

    return nil
}
