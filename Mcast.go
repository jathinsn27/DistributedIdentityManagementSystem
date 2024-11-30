package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	pb "spanningtree/proto"
)

const (
	etcdPrefix = "/nodes/"
)

type Node struct {
	ID       string
	Address  string
	Parent   *Node
	Children []*Node
	mu       sync.RWMutex
}

type SpanningTree struct {
	Root *Node
	mu   sync.RWMutex
}

var (
	node      *Node
	tree      *SpanningTree
	etcdCli   *clientv3.Client
	grpcServer *grpc.Server
)

func (n *Node) SendMessage(ctx context.Context, msg *pb.Message) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	log.Printf("Node %s received message: %s", n.ID, msg.Content)

	for _, child := range n.Children {
		go func(childNode *Node) {
			conn, err := grpc.Dial(childNode.Address, grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to child %s: %v", childNode.ID, err)
				return
			}
			defer conn.Close()

			client := pb.NewMulticastServiceClient(conn)
			_, err = client.SendMessage(ctx, msg)
			if err != nil {
				log.Printf("Failed to send message to child %s: %v", childNode.ID, err)
			}
		}(child)
	}

	return nil
}

func (s *SpanningTree) AddNode(nodeID, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newNode := &Node{ID: nodeID, Address: address}

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

func joinCluster(cli *clientv3.Client, node *Node) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Put(ctx, etcdPrefix+node.ID, node.Address)
	return err
}

func watchMembership(cli *clientv3.Client) {
	watcher := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
	for response := range watcher {
		for _, ev := range response.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				nodeID := string(ev.Kv.Key[len(etcdPrefix):])
				address := string(ev.Kv.Value)
				tree.AddNode(nodeID, address)
				log.Printf("Node added: %s at %s", nodeID, address)
			case clientv3.EventTypeDelete:
				nodeID := string(ev.Kv.Key[len(etcdPrefix):])
				tree.RemoveNode(nodeID)
				log.Printf("Node removed: %s", nodeID)
			}
		}
	}
}

type server struct {
	pb.UnimplementedMulticastServiceServer
}

func (s *server) SendMessage(ctx context.Context, msg *pb.Message) (*pb.Empty, error) {
	err := node.SendMessage(ctx, msg)
	return &pb.Empty{}, err
}

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID environment variable is not set")
	}

	etcdEndpoints := strings.Split(os.Getenv("ETCD_ENDPOINTS"), ",")
	if len(etcdEndpoints) == 0 {
		log.Fatal("ETCD_ENDPOINTS environment variable is not set")
	}

	var err error
	etcdCli, err = clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer etcdCli.Close()

	node = &Node{ID: nodeID, Address: nodeID + ":50051"}
	tree = &SpanningTree{}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer = grpc.NewServer()
	pb.RegisterMulticastServiceServer(grpcServer, &server{})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	if err := joinCluster(etcdCli, node); err != nil {
		log.Fatal("Error joining cluster:", err)
	}

	go watchMembership(etcdCli)

	// Keep the node alive
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := etcdCli.Put(ctx, etcdPrefix+node.ID, node.Address)
			cancel()
			if err != nil {
				log.Printf("Error keeping node alive: %v", err)
			}
			time.Sleep(5 * time.Second)
		}
	}()

	// Simulate random multicast
	for {
		time.Sleep(time.Duration(rand.Intn(10)+5) * time.Second)
		if tree.Root != nil && tree.Root.ID == nodeID {
			msg := &pb.Message{Content: fmt.Sprintf("Hello from %s", nodeID)}
			err := node.SendMessage(context.Background(), msg)
			if err != nil {
				log.Printf("Error multicasting message: %v", err)
			}
		}
	}
}