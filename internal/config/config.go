// Package config holds the server configuration and the logic to build it
// from CLI flags + environment variables. Env var names mirror MinIO's
// (MINIO_*) but with the GOSTORE_ prefix.
package config

import (
	"errors"
	"os"
	"strings"
)

// Config is the fully-resolved server configuration.
type Config struct {
	// Address is the listen address for the S3 API server, e.g. ":9000".
	Address string
	// ConsoleAddress is the listen address for the web console, e.g. ":9001".
	ConsoleAddress string
	// Volumes are the disk paths (or expanded ellipsis specs) backing storage.
	// A single path => single-disk mode. Multiple => erasure mode (M4+).
	Volumes []string

	// Region reported by the S3 API (LocationConstraint).
	Region string

	// RootUser / RootPassword are the bootstrap admin credentials, analogous
	// to MINIO_ROOT_USER / MINIO_ROOT_PASSWORD. Signature verification (M2)
	// and IAM (M8) build on top of these.
	RootUser     string
	RootPassword string

	// LogLevel is one of debug|info|warn|error. LogJSON toggles JSON output.
	LogLevel string
	LogJSON  bool
}

// Default returns a Config with sane defaults, before flags/env are applied.
func Default() Config {
	return Config{
		Address:        ":9000",
		ConsoleAddress: ":9001",
		Region:         "us-east-1",
		RootUser:       "gostoreadmin",
		RootPassword:   "gostoreadmin",
		LogLevel:       "info",
	}
}

// ApplyEnv overlays GOSTORE_* environment variables onto c (env wins over
// defaults, but CLI flags — applied by the caller afterwards — win over env
// only when explicitly set; for M0 we keep it simple: env overrides here).
func (c *Config) ApplyEnv() {
	if v := os.Getenv("GOSTORE_ADDRESS"); v != "" {
		c.Address = v
	}
	if v := os.Getenv("GOSTORE_CONSOLE_ADDRESS"); v != "" {
		c.ConsoleAddress = v
	}
	if v := os.Getenv("GOSTORE_REGION"); v != "" {
		c.Region = v
	}
	if v := os.Getenv("GOSTORE_ROOT_USER"); v != "" {
		c.RootUser = v
	}
	if v := os.Getenv("GOSTORE_ROOT_PASSWORD"); v != "" {
		c.RootPassword = v
	}
	if v := os.Getenv("GOSTORE_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
	if v := os.Getenv("GOSTORE_LOG_JSON"); v == "1" || strings.EqualFold(v, "true") {
		c.LogJSON = true
	}
}

// Validate checks the config is internally consistent and usable.
func (c Config) Validate() error {
	if c.Address == "" {
		return errors.New("config: empty API address")
	}
	if len(c.Volumes) == 0 {
		return errors.New("config: no storage volumes given (usage: gostore server [flags] DIR...)")
	}
	if len(c.RootUser) < 3 || len(c.RootPassword) < 8 {
		return errors.New("config: root user must be >=3 chars and root password >=8 chars")
	}
	// Erasure mode needs an even count >=4 (M4 will refine: 4/6/8/10/12/14/16).
	if len(c.Volumes) > 1 && (len(c.Volumes)%2 != 0 || len(c.Volumes) < 4) {
		return errors.New("config: erasure mode requires an even number of volumes, minimum 4")
	}
	return nil
}

// SingleDisk reports whether the server runs in single-disk mode.
func (c Config) SingleDisk() bool { return len(c.Volumes) == 1 }
