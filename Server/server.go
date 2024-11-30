package server

import (
    "context"

	mn "DistributedIdentityManagementSystem/main"
    pb "DistributedIdentityManagementSystem/proto"
)

type Server struct {
    pb.UnimplementedMulticastServiceServer
    Node *mn.Node
}

func (s *Server) SendMessage(ctx context.Context, msg *pb.Message) (*pb.Empty, error) {
    err := s.mn.Node.SendMessage(ctx, msg)
    return &pb.Empty{}, err
}