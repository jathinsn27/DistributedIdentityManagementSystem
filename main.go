package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
	"strings"

	"go.etcd.io/etcd/client/v3"
	"github.com/hashicorp/memberlist"
)

const (
	etcdPrefix = "/members/"
)

type Node struct {
    ID       string
    Address  string
}

type MulticastMessage struct {
    SenderID string
    Content  string
}

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		log.Fatal("NODE_ID environment variable is not set")
	}

	// Connect to etcd
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

    // Join the cluster
    node := Node{ID: nodeID, Address: os.Getenv("NODE_ADDRESS")}
    if err := joinCluster(cli, node); err != nil {
        log.Fatal(err)
    }
	fmt.Printf("%s joined the cluster\n", nodeID)

	

	// Create memberlist config
    config := memberlist.DefaultLocalConfig()
    config.Name = nodeID
    config.BindAddr = strings.Split(node.Address, ":")[0]
    config.BindPort = 7946 // Default memberlist port

    // Create memberlist
    list, err := memberlist.Create(config)
    if err != nil {
        log.Fatal("Failed to create memberlist: ", err)
    }

    // Join the memberlist cluster
    _, err = list.Join([]string{node.Address})
    if err != nil {
        log.Fatal("Failed to join memberlist cluster: ", err)
    }

    fmt.Printf("Current memberlist: %v\n", list.Members())

	// Watch for membership changes
	go watchMembership(cli, list)

    go logMemberlist(list)

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

func joinCluster(cli *clientv3.Client, node Node) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _, err := cli.Put(ctx, etcdPrefix+node.ID, node.Address)
    return err
}

func watchMembership(cli *clientv3.Client, list *memberlist.Memberlist) {
    watcher := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
    for response := range watcher {
        for _, ev := range response.Events {
            switch ev.Type {
            case clientv3.EventTypePut:
                nodeID := string(ev.Kv.Key)[len(etcdPrefix):]
                address := string(ev.Kv.Value)
                fmt.Printf("Node joined: %s at %s\n", nodeID, address)
                _, err := list.Join([]string{address})
                if err != nil {
                    log.Printf("Failed to add node to memberlist: %v", err)
                }
            case clientv3.EventTypeDelete:
                nodeID := string(ev.Kv.Key)[len(etcdPrefix):]
                fmt.Printf("Node left: %s\n", nodeID)
            }
        }
    }
}

func logMemberlist(list *memberlist.Memberlist) {
    for {
        log.Printf("Current memberlist: %v\n", list.Members())
        time.Sleep(30 * time.Second)
    }
}