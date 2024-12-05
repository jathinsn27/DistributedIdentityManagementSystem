package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
)

type MulticastMessage struct {
	Query string        `json:"query"`
	Args  []interface{} `json:"args"`
}

func multicast(query string, args []interface{}, nodeId string) error {
	tree := GetGlobalTree()
	members, e := getMembershipList(os.Getenv("MEMBERSHIP_HOST"))
	if e != nil {
		return fmt.Errorf("failed to query membershipList")
	}
	membersList := make([]string, 0, len(members))
	for k := range members {
		membersList = append(membersList, k)
	}
	leader, e := GetLeaderNode(members)
	fmt.Printf("prev list %v \n", prevMembershipList)
	fmt.Printf("curr list %v \n", membersList)

	if e != nil {
		return fmt.Errorf("failed to get leader to construct tree")
	}
	if tree.Root == nil {
		err := ConstructSpanningTree(tree, members, leader.ID)
		if err != nil {
			return fmt.Errorf("failed to construct spanning tree: %v", err)
		}
	} else {
		for _, prevMember := range prevMembershipList {
			if members[prevMember] == nil {
				// Delete all the nodes that have died or left the cluster
				fmt.Printf("Remove node : %s\n", prevMember)
				tree.RemoveNode(prevMember, leader.ID)
			}
		}
		sort.Strings(prevMembershipList)
		for member := range members {
			index := sort.SearchStrings(prevMembershipList, member)
			found := index < len(prevMembershipList) && prevMembershipList[index] == member
			fmt.Printf("found : %v , %s", found, member)
			if found != true {
				fmt.Printf("Add node : %s\n", member)
				tree.AddNode(member, members[member].Address, leader.ID)
			}
		}
	}

	// Print the tree
	tree.PrintTree()

	prevMembershipList = membersList
	fmt.Printf("Inside Multicast\n")

	msg := MulticastMessage{
		Query: query,
		Args:  args,
	}

	fmt.Printf("Multicasting node : %s\n", nodeId)
	multicastNode := tree.Root.FindNodeDFS(nodeId)
	if multicastNode == nil {
		return nil
	}
	return multicastToChildren(multicastNode, msg)
}

func multicastToChildren(node *SpanningTreeNode, msg MulticastMessage) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(node.Children))

	for _, child := range node.Children {
		if child == nil {
			continue // Skip nil children
		}
		wg.Add(1)
		go func(childNode *SpanningTreeNode) {
			defer wg.Done()
			if err := sendMulticast(childNode.address, msg); err != nil {
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
	fmt.Println("Send Multicast")
	jsonData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal multicast message: %v", err)
	}

	fmt.Printf("http://%s/recvMulticast", address)
	fmt.Println(bytes.NewBuffer(jsonData))
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
	fmt.Printf("Received Multicast\n")
	var msg MulticastMessage

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		fmt.Printf("Error decoding multicast message: %v", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(msg.Query, msg.Args...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	fmt.Printf("Received Multicast Message: Query = %s, Args = %v", msg.Query, msg.Args)

	NodeID := os.Getenv("NODE_ID")
	go multicast(msg.Query, msg.Args, NodeID)

	w.WriteHeader(http.StatusOK)
}
