package tree

import (
	"sync"

	"spanningtree/internal/node"
)

type SpanningTree struct {
	Root *node.Node
	mu   sync.RWMutex
}

func (s *SpanningTree) AddNode(nodeID, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newNode := &node.Node{ID: nodeID, Address: address}

	if s.Root == nil {
		s.Root = newNode
		return
	}

	parent := s.findParent()
	parent.Children = append(parent.Children, newNode)
	newNode.Parent = parent
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

	var removeNodeRecursive func(*node.Node) bool
	removeNodeRecursive = func(n *node.Node) bool {
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

func (s *SpanningTree) findParent() *node.Node {
	var findNodeWithFewestChildren func(*node.Node) *node.Node
	findNodeWithFewestChildren = func(n *node.Node) *node.Node {
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