package database

import (
	"context"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AnalyticsServiceDB struct {
	Db *mongo.Database
}

func NewAnalyticsServiceDB() (*AnalyticsServiceDB, error) {
	// read the environment variables
	user := os.Getenv("MONGODB_USER")
	pass := os.Getenv("MONGODB_PASSWORD")
	host := os.Getenv("MONGODB_HOST")
	port := os.Getenv("MONGODB_PORT")
	dbName := os.Getenv("MONGODB_DB")

	// create the connection string
	connectionString := fmt.Sprintf("mongodb://%s:%s@%s:%s/?retryWrites=true&w=majority", user, pass, host, port)
	fmt.Println(connectionString)
	// connect to the database
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, err
	}

	return &AnalyticsServiceDB{Db: client.Database(dbName)}, nil
}
