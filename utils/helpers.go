package utils

import (
	"os"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func LoadEnvVarsFromFile() error {
	// get the runnig environment
	environment := GetEnvVar("GO_ENV", "development")

	// if it's development load the .env file
	if environment == "development" {
		err := godotenv.Load()
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

func GetEnvVar(key string, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

// StringToProtoTimestamp converts the string to a protobuf timestamp
func StringToProtoTimestamp(timestamp string) (*timestamppb.Timestamp, error) {
	t, err := time.Parse(time.DateTime, timestamp)
	if err != nil {
		return nil, err
	}

	return timestamppb.New(t), nil
}
