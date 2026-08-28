package main

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

type productionConfig struct {
	ListenAddress string
	PostgresDSN   string
	RedisAddress  string
	RedisPassword string
	RedisDB       int
	SecretRoot    string

	S3Region, S3Bucket, S3Endpoint string
	S3PathStyle, S3AllowInsecure   bool
	S3MaxBytes                     int64

	ClamAVAddress                             string
	DLPEndpoint, DLPProbeTenant, DLPSecretRef string
	DLPSecretVersion, DLPBackendVersion       int64
	DLPAllowInsecure                          bool

	ProbeTimeout, ProbeInterval, ShutdownTimeout time.Duration
	ArtifactPutTimeout, UploadProtection         time.Duration
	UploadClaimTTL, ArtifactClaimTTL             time.Duration
	UploadPollInterval, ArtifactPollInterval     time.Duration
	ArtifactOrphanGrace                          time.Duration
	LifecycleBatchSize, LifecycleMaxAttempts     int
}

func loadProductionConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress:        valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:          strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		RedisAddress:         strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword:        getenv("TRPC_REDIS_PASSWORD"),
		SecretRoot:           strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		S3Region:             strings.TrimSpace(getenv("TRPC_S3_REGION")),
		S3Bucket:             strings.TrimSpace(getenv("TRPC_S3_BUCKET")),
		S3Endpoint:           strings.TrimSpace(getenv("TRPC_S3_ENDPOINT")),
		ClamAVAddress:        strings.TrimSpace(getenv("TRPC_CLAMAV_ADDRESS")),
		DLPEndpoint:          strings.TrimSpace(getenv("TRPC_DLP_ENDPOINT")),
		DLPProbeTenant:       strings.TrimSpace(getenv("TRPC_DLP_PROBE_TENANT_ID")),
		DLPSecretRef:         strings.TrimSpace(getenv("TRPC_DLP_SECRET_REF")),
		ProbeTimeout:         5 * time.Second,
		ProbeInterval:        15 * time.Second,
		ShutdownTimeout:      30 * time.Second,
		ArtifactPutTimeout:   30 * time.Second,
		UploadProtection:     2 * time.Minute,
		UploadClaimTTL:       time.Minute,
		ArtifactClaimTTL:     time.Minute,
		UploadPollInterval:   time.Minute,
		ArtifactPollInterval: time.Minute,
		ArtifactOrphanGrace:  24 * time.Hour,
		LifecycleBatchSize:   100,
		LifecycleMaxAttempts: 8,
		S3MaxBytes:           16 << 20,
	}
	var err error
	if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
		return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
	}
	if config.DLPSecretVersion, err = envInt64(getenv, "TRPC_DLP_SECRET_VERSION", 0); err != nil || config.DLPSecretVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_DLP_SECRET_VERSION")
	}
	if config.DLPBackendVersion, err = envInt64(getenv, "TRPC_DLP_BACKEND_VERSION", 0); err != nil || config.DLPBackendVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_DLP_BACKEND_VERSION")
	}
	for name, target := range map[string]*bool{
		"TRPC_S3_PATH_STYLE": &config.S3PathStyle, "TRPC_S3_ALLOW_INSECURE": &config.S3AllowInsecure,
		"TRPC_DLP_ALLOW_INSECURE": &config.DLPAllowInsecure,
	} {
		if *target, err = envBool(getenv, name, false); err != nil {
			return productionConfig{}, errors.New("invalid " + name)
		}
	}
	durations := []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_ARTIFACT_PUT_TIMEOUT", &config.ArtifactPutTimeout, time.Second},
		{"TRPC_ARTIFACT_UPLOAD_PROTECTION", &config.UploadProtection, time.Second},
		{"TRPC_ARTIFACT_UPLOAD_CLAIM_TTL", &config.UploadClaimTTL, time.Second},
		{"TRPC_ARTIFACT_RETENTION_CLAIM_TTL", &config.ArtifactClaimTTL, time.Second},
		{"TRPC_ARTIFACT_UPLOAD_POLL_INTERVAL", &config.UploadPollInterval, time.Millisecond},
		{"TRPC_ARTIFACT_RETENTION_POLL_INTERVAL", &config.ArtifactPollInterval, time.Millisecond},
		{"TRPC_ARTIFACT_ORPHAN_GRACE", &config.ArtifactOrphanGrace, time.Minute},
	}
	for _, item := range durations {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.LifecycleBatchSize, err = envInt(getenv, "TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE", config.LifecycleBatchSize); err != nil || config.LifecycleBatchSize < 1 || config.LifecycleBatchSize > 1000 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE")
	}
	if config.LifecycleMaxAttempts, err = envInt(getenv, "TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS", config.LifecycleMaxAttempts); err != nil || config.LifecycleMaxAttempts < 1 || config.LifecycleMaxAttempts > 100 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS")
	}
	maxBytes, err := envInt64(getenv, "TRPC_ARTIFACT_MAX_BYTES", config.S3MaxBytes)
	if err != nil || maxBytes < 1 || uint64(maxBytes) > uint64(^uint(0)>>1) {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_MAX_BYTES")
	}
	config.S3MaxBytes = maxBytes
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.RedisAddress == "" || config.SecretRoot == "" || config.S3Region == "" ||
		config.S3Bucket == "" || config.ClamAVAddress == "" ||
		config.DLPEndpoint == "" || config.DLPProbeTenant == "" || config.DLPSecretRef == "" {
		return productionConfig{}, errors.New("required production dependency configuration is missing")
	}
	if config.ArtifactPutTimeout >= config.UploadProtection {
		return productionConfig{}, errors.New("artifact put timeout must be shorter than upload protection")
	}
	return config, nil
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envBool(getenv func(string) string, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseBool(value)
}

func envDuration(getenv func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func envInt(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func envInt64(getenv func(string) string, name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseInt(value, 10, 64)
}
