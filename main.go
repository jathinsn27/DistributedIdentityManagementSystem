package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "go.etcd.io/etcd/client/v3"
)

const (
    etcdPrefix = "/members/"
)

func main() {
    nodeID := os.Getenv("NODE_ID")
    if nodeID == "" {
        log.Fatal("NODE_ID environment variable is not set")
    }

    // Connect to etcd
    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"etcd:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close()

    // Join the cluster
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    _, err = cli.Put(ctx, etcdPrefix+nodeID, nodeID)
    cancel()
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("%s joined the cluster\n", nodeID)

    // Watch for membership changes
    go watchMembership(cli)

    // Keep the node alive
    for {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        _, err = cli.Put(ctx, etcdPrefix+nodeID, nodeID)
        cancel()
        if err != nil {
            log.Printf("Error keeping node alive: %v", err)
        }
        time.Sleep(5 * time.Second)
    }
}

func watchMembership(cli *clientv3.Client) {
    watcher := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
    for response := range watcher {
        for _, ev := range response.Events {
            switch ev.Type {
            case clientv3.EventTypePut:
                fmt.Printf("Node joined: %s\n", string(ev.Kv.Value))
            case clientv3.EventTypeDelete:
                fmt.Printf("Node left: %s\n", string(ev.Kv.Key))
            }
        }
    }
}