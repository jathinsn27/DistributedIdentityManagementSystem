# DistributedIdentityManagementSystem
An Identity management system that is used to track ID's region wise. The system allows admins to create/assign, update and delete roles/access to users. 

## System Design
+ Membership List: Membership Lists are used to keep track of which processes to send the multicast messages to. 
  + Membership List runs as an individual service where all nodes initially send a hearbeat to the server.
  + A key is created w.r.t to that node in the server and a lease is attached to it with an expiry time.
    + The lease is automatically renewed when successive heartbeats are recieved.
    + The node is removed if it does not send a fresh heartbeat before its lease expires.

+ Leader Election: 
  + Uses Quorum based voting.
  + Implements a raft like consensus algorithm to identify the leader.

+ Mutlicast By Spanning Tree : The updates made on the leader copy of the database is multicast to all the other replica nodes by mutlicasting the data in the spanning tree.
  + The algorithm makes sure that the leader is always at the root of the tree and balances the tree using AVL tree algorithm. In this way the tree is constructed consistently across all nodes.

+ Consistency: Whenever a new node joins or an existing node recovers it syncs with the current leader by requesting its trnasaction logs and updating it locally.

## Deployment
+ For ease of testing all all the services are defined as part of a single docker compose file.
+ To run the system
```
docker compose up --build
```

