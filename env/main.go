package env

import (
	"fmt"
	"os"
	"strconv"
)

func String(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}

	return fallback
}

func Bool(name string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}

	out, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}

	return out, nil
}
