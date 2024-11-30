package node

import (
    "context"
    "log"
    "sync"
	mn "DistributedIdentityManagementSystem/main"

    "google.golang.org/grpc"
    pb "spanningtree/proto"
)

func (n *mn.Node) SendMessage(ctx context.Context, msg *pb.Message) error {
    n.mu.RLock()
    defer n.mu.RUnlock()

    log.Printf("Node %s received message: %s", n.ID, msg.Content)

    for _, child := range n.Children {
        go func(childNode *mn.Node) {
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