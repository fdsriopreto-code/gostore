// Package config holds the server configuration and the logic to build it
// from CLI flags + environment variables. Env var names mirror MinIO's
// (MINIO_*) but with the GOSTORE_ prefix.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// Config is the fully-resolved server configuration.
type Config struct {
	// Address is the listen address for the S3 API server, e.g. ":9000".
	Address string
	// ConsoleAddress is the listen address for the web console, e.g. ":9001".
	ConsoleAddress string
	// Volumes is the flat list of every disk path (all groups concatenated),
	// used for logging and single-disk detection.
	Volumes []string

	// VolumeGroups is the disk paths grouped by CLI argument. Each group with
	// more than one disk becomes one erasure set; all groups together form a
	// single pool (M5). One group of one disk => single-disk mode.
	VolumeGroups [][]string

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
	if len(c.VolumeGroups) == 0 {
		return errors.New("config: no storage volumes given (usage: gostore server [flags] DIR...)")
	}
	if len(c.RootUser) < 3 || len(c.RootPassword) < 8 {
		return errors.New("config: root user must be >=3 chars and root password >=8 chars")
	}
	if c.SingleDisk() {
		return nil
	}
	for i, g := range c.VolumeGroups {
		if len(g) < 4 || len(g)%2 != 0 {
			return fmt.Errorf("config: erasure set %d has %d disks; each set needs an even count >= 4", i+1, len(g))
		}
	}
	return nil
}

// SingleDisk reports whether the server runs in single-disk mode.
func (c Config) SingleDisk() bool {
	return len(c.VolumeGroups) == 1 && len(c.VolumeGroups[0]) == 1
}
