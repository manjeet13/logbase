package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPPort              string
	DataDir               string
	MemTableFlushSize     int
	MaxSSTablesBeforeComp int
}

func Load() *Config {
	return &Config{
		HTTPPort:              getEnv("LOGBASE_HTTP_PORT", "8080"),
		DataDir:               getEnv("LOGBASE_DATA_DIR", "data"),
		MemTableFlushSize:     getEnvAsInt("LOGBASE_MEMTABLE_FLUSH_BYTES", 1024*1024),
		MaxSSTablesBeforeComp: getEnvAsInt("LOGBASE_MAX_SSTABLES", 4),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return defaultVal
}
