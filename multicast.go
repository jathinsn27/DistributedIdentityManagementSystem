package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "sync"
)

type MulticastMessage struct {
    Query string        `json:"query"`
    Args  []interface{} `json:"args"`
}

func multicast(query string, args []interface{}) error {
    tree := GetGlobalTree()
    if tree.Root == nil {
        err := ConstructSpanningTree(tree, os.Getenv("MEMBERSHIP_HOST"))
        if err != nil {
            return fmt.Errorf("failed to construct spanning tree: %v", err)
        }
    }

    msg := MulticastMessage{
        Query: query,
        Args:  args,
    }

    return multicastToChildren(tree.Root, msg)
}

func multicastToChildren(node *Node, msg MulticastMessage) error {
    var wg sync.WaitGroup
    errChan := make(chan error, len(node.Children))

    for _, child := range node.Children {
        wg.Add(1)
        go func(childNode *Node) {
            defer wg.Done()
            if err := sendMulticast(childNode.Address, msg); err != nil {
                errChan <- fmt.Errorf("failed to multicast to %s: %v", childNode.ID, err)
            }
        }(child)
    }

    go func() {
        wg.Wait()
        close(errChan)
    }()

    for err := range errChan {
        if err != nil {
            return err
        }
    }

    return nil
}

func sendMulticast(address string, msg MulticastMessage) error {
    jsonData, err := json.Marshal(msg)
    if err != nil {
        return fmt.Errorf("failed to marshal multicast message: %v", err)
    }

    resp, err := http.Post(fmt.Sprintf("http://%s/recvMulticast", address), "application/json", bytes.NewBuffer(jsonData))
    if err != nil {
        return fmt.Errorf("failed to send multicast: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("multicast failed with status: %s", resp.Status)
    }

    return nil
}

func recvMulticast(w http.ResponseWriter, r *http.Request) {
    var msg MulticastMessage

    err := json.NewDecoder(r.Body).Decode(&msg)
    if err != nil {
        fmt.Printf("Error decoding multicast message: %v", err)
        http.Error(w, "Bad request", http.StatusBadRequest)
        return
    }

    fmt.Printf("Received Multicast Message: Query = %s, Args = %v", msg.Query, msg.Args)

    w.WriteHeader(http.StatusOK)
}

