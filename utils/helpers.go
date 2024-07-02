package utils

import (
	"os"

	"github.com/joho/godotenv"
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
