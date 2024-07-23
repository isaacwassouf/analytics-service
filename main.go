package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	db "github.com/isaacwassouf/analytics-service/database"
	pb "github.com/isaacwassouf/analytics-service/protobufs/analytics_service"
	"github.com/isaacwassouf/analytics-service/utils"
)

type AnalyticsService struct {
	pb.UnimplementedAnalyticsServiceServer
	analyticsServiceDB *db.AnalyticsServiceDB
}

func (s *AnalyticsService) Log(ctx context.Context, in *pb.LogRequest) (*pb.LogResponse, error) {
	// parse the metadata into a map
	var metadata map[string]interface{}
	err := json.Unmarshal([]byte(in.LogEntry.Metadata), &metadata)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "failed to parse metadata")
	}

	collection := s.analyticsServiceDB.Db.Collection("logs")
	_, err = collection.InsertOne(ctx, bson.M{
		"service":    in.LogEntry.ServiceName,
		"level":      in.LogEntry.Level,
		"message":    in.LogEntry.ResponseMessage,
		"metadata":   metadata,
		"created_at": time.Now(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.LogResponse{Message: "Log entry added successfully"}, nil
}

func (s *AnalyticsService) ListLogs(ctx context.Context, in *pb.ListLogsRequest) (*pb.ListLogsResponse, error) {
	collection := s.analyticsServiceDB.Db.Collection("logs")
	filter := bson.M{}
	now := time.Now()

	switch in.Window {
	case pb.Window_YESTERDAY:
		filter["created_at"] = bson.M{
			"$gte": now.AddDate(0, 0, -1).Truncate(24 * time.Hour),
			"$lt":  now.Truncate(24 * time.Hour),
		}
	case pb.Window_TODAY:
		filter["created_at"] = bson.M{
			"$gte": now.Truncate(24 * time.Hour),
		}
	case pb.Window_LAST_WEEK:
		filter["created_at"] = bson.M{
			"$gte": now.AddDate(0, 0, -7),
		}
	case pb.Window_LAST_MONTH:
		filter["created_at"] = bson.M{
			"$gte": now.AddDate(0, -1, 0),
		}
	case pb.Window_LAST_THREE_MONTHS:
		filter["created_at"] = bson.M{
			"$gte": now.AddDate(0, -3, 0),
		}
	}

	cursor, err := collection.Find(ctx, filter, options.Find().SetSort(bson.M{"created_at": -1}))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer cursor.Close(ctx)

	var logs []*pb.LogEntry
	// define a struct to hold the database log details
	var logDetails struct {
		Service   string                 `bson:"service"`
		Level     string                 `bson:"level"`
		Message   string                 `bson:"message"`
		Metadata  map[string]interface{} `bson:"metadata"`
		CreatedAt time.Time              `bson:"created_at"`
	}
	for cursor.Next(ctx) {
		err := cursor.Decode(&logDetails)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		metadataBytes, err := json.Marshal(logDetails.Metadata)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to marshal metadata")
		}

		logs = append(logs, &pb.LogEntry{
			ServiceName:     logDetails.Service,
			Level:           logDetails.Level,
			ResponseMessage: logDetails.Message,
			Metadata:        string(metadataBytes),
			CreatedAt:       logDetails.CreatedAt.Format(time.RFC3339),
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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
	err = analyticsServiceDB.Db.Client().Ping(context.Background(), nil)
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
