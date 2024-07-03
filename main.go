package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"

	sq "github.com/Masterminds/squirrel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	db "github.com/isaacwassouf/analytics-service/database"
	pb "github.com/isaacwassouf/analytics-service/protobufs/analytics_service"
	"github.com/isaacwassouf/analytics-service/utils"
)

type AnalyticsService struct {
	pb.UnimplementedAnalyticsServiceServer
	analyticsServiceDB *db.AnalyticsServiceDB
}

func (s *AnalyticsService) Log(ctx context.Context, in *pb.LogRequest) (*pb.LogResponse, error) {
	_, err := sq.Insert("logs").
		Columns("service", "level", "message", "metadata").
		Values(in.LogEntry.ServiceName, in.LogEntry.Level, in.LogEntry.ResponseMessage, in.LogEntry.Metadata).
		RunWith(s.analyticsServiceDB.Db).
		Exec()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.LogResponse{Message: "Log entry added successfully"}, nil
}

func (s *AnalyticsService) ListLogs(ctx context.Context, in *emptypb.Empty) (*pb.ListLogsResponse, error) {
	rows, err := sq.Select("service", "level", "message", "metadata", "created_at").
		From("logs").
		OrderBy("created_at DESC").
		RunWith(s.analyticsServiceDB.Db).Query()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	defer rows.Close()

	var logs []*pb.LogEntry
	for rows.Next() {
		var service, level string
		var createdAt string
		var message sql.NullString
		var metadata interface{}

		err := rows.Scan(&service, &level, &message, &metadata, &createdAt)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		// convert the metadata to bypes and then to string
		metadataBytes, ok := metadata.([]byte)
		if !ok {
			return nil, status.Error(codes.Internal, "failed to convert metadata to bytes")
		}
		metadata = string(metadataBytes)

		// convert the created string to a proto timestamp
		createdTime, err := utils.StringToProtoTimestamp(createdAt)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		logs = append(logs, &pb.LogEntry{
			ServiceName:     service,
			Level:           level,
			ResponseMessage: message.String,
			Metadata:        metadata.(string),
			Timestamp:       createdTime,
		})
	}

	return &pb.ListLogsResponse{Logs: logs}, nil
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
