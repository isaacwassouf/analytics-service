package utils

import (
	"os"
	"time"

	"github.com/joho/godotenv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func LoadEnvVarsFromFile() error {
	err := godotenv.Load()
	if err != nil {
		return err
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
