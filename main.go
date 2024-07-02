package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	db "github.com/isaacwassouf/analytics-service/database"
	pb "github.com/isaacwassouf/analytics-service/protobufs/analytics_service"
	"github.com/isaacwassouf/analytics-service/utils"
)

type AnalyticsService struct {
	pb.UnimplementedAnalyticsServiceServer
	analyticsServiceDB *db.AnalyticsServiceDB
}

func (s *AnalyticsService) Log(ctx context.Context, in *pb.LogRequest) (*pb.LogResponse, error) {
	return &pb.LogResponse{Message: "Logged"}, nil
}

func main() {
	// load the environment variables from the .env file
	err := utils.LoadEnvVarsFromFile()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// Create a new schemaManagementServiceDB
	analyticsServiceDB, err := db.NewAnalyticsServiceDB()
	if err != nil {
		log.Fatalf("failed to create a new SchemaManagementServiceDB: %v", err)
	}
	// ping the database
	err = analyticsServiceDB.Db.Ping()
	if err != nil {
		log.Fatalf("failed to ping the database: %v", err)
	}

	// Start the server
	ls, err := net.Listen("tcp", ":8089")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterAnalyticsServiceServer(s, &AnalyticsService{
		analyticsServiceDB: analyticsServiceDB,
	})

	log.Printf("Server listening at %v", ls.Addr())

	if err := s.Serve(ls); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
