import (
    "net"
    "net/rpc"
    "sync"
)

type MulticastService struct{}

type MulticastArgs struct {
    Message string
}

type MulticastReply struct {
    Received bool
}

func (m *MulticastService) ReceiveMessage(args *MulticastArgs, reply *MulticastReply) error {
    fmt.Printf("Received multicast message: %s\n", args.Message)
    reply.Received = true
    return nil
}

func startMulticasting(list *memberlist.Memberlist) {
    // Start RPC server
    multicastService := &MulticastService{}
    rpc.Register(multicastService)
    listener, err := net.Listen("tcp", ":8000")
    if err != nil {
        log.Fatal("ListenTCP error:", err)
    }
    go rpc.Accept(listener)

    for {
        message := fmt.Sprintf("Multicast from %s at %s", list.LocalNode().Name, time.Now())
        
        var wg sync.WaitGroup
        for _, member := range list.Members() {
            if member.Name != list.LocalNode().Name {
                wg.Add(1)
                go func(memberAddr string) {
                    defer wg.Done()
                    client, err := rpc.Dial("tcp", memberAddr+":8000")
                    if err != nil {
                        log.Printf("RPC Dial error: %v", err)
                        return
                    }
                    defer client.Close()

                    args := &MulticastArgs{Message: message}
                    var reply MulticastReply
                    err = client.Call("MulticastService.ReceiveMessage", args, &reply)
                    if err != nil {
                        log.Printf("RPC Call error: %v", err)
                    } else if reply.Received {
                        fmt.Printf("Message sent to %s\n", memberAddr)
                    }
                }(member.Addr.String())
            }
        }
        wg.Wait()

        time.Sleep(10 * time.Second)
    }
}