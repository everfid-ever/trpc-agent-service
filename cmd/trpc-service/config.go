package main

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const workerHTTPShutdownBudget = 5 * time.Second

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
	PayloadKeyRef                             string
	PayloadKeyVersion                         int64
	DLPAllowInsecure                          bool

	ProbeTimeout, ProbeInterval, ShutdownTimeout time.Duration
	ArtifactPutTimeout, UploadProtection         time.Duration
	UploadClaimTTL, ArtifactClaimTTL             time.Duration
	UploadPollInterval, ArtifactPollInterval     time.Duration
	ArtifactOrphanGrace                          time.Duration
	LifecycleBatchSize, LifecycleMaxAttempts     int

	PreprocessBatchSize, PreprocessMaxAttempts int
	PreprocessLeaseTTL, PreprocessRetryDelay   time.Duration
	PreprocessPollInterval, ArtifactRetention  time.Duration
	MediaFetchTimeout                          time.Duration
	MediaAllowedHosts                          []string

	ChannelCandidateTTL                                  time.Duration
	ChannelCallbackMaxBody                               int64
	ChannelProbeTenant                                   string
	RedisEnvironment                                     string
	ChannelDeliveryGroup                                 string
	ChannelDeliveryRefresh                               time.Duration
	ChannelReplyReadBlock                                time.Duration
	ChannelReplyReclaimIdle, ChannelReplyReclaimInterval time.Duration
	ChannelDeliveryClaimTTL, ChannelDeliveryClaimRenew   time.Duration
	ChannelDeliveryRetryDelay, ChannelDeliveryMaxRetry   time.Duration
	ChannelProviderTimeout                               time.Duration
	ChannelReplyReclaimLimit, ChannelDeliveryMaxAttempts int
	ChannelDeliveryMaxReconcile                          int
	WebUIEnabled                                         bool

	WorkerID, WorkerGroup, WorkerControlGroup, WorkerProbeTenant string
	WorkerShardCount, WorkerReclaimLimit                         int
	WorkerShards                                                 []uint32
	WorkerLeaseTTL, WorkerLeaseRenew, WorkerRetryWait            time.Duration
	WorkerReclaimInterval, WorkerCancelPoll, WorkerDrainTimeout  time.Duration
	WorkerBundleFailureBackoff, WorkerBundleCloseTimeout         time.Duration

	GatewayProbeTenant, GatewayAuthSecretRef, GatewayPublicURL           string
	GatewayAuthSecretVersion, GatewayMaxBody, GatewaySSEReplayLimit      int64
	GatewayAuthClockSkew, GatewaySSEPollInterval, GatewayProtocolTimeout time.Duration
	GatewaySSEMaxSubscribers                                             int64
}

func loadGatewayConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:   strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		PayloadKeyRef: strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")), GatewayProbeTenant: strings.TrimSpace(getenv("TRPC_GATEWAY_PROBE_TENANT_ID")),
		GatewayAuthSecretRef: strings.TrimSpace(getenv("TRPC_GATEWAY_AUTH_SECRET_REF")),
		GatewayPublicURL:     strings.TrimSpace(getenv("TRPC_GATEWAY_PUBLIC_BASE_URL")),
		ProbeTimeout:         5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 45 * time.Second,
		GatewayAuthClockSkew: 30 * time.Second, GatewayMaxBody: 1 << 20, GatewaySSEPollInterval: time.Second,
		GatewaySSEReplayLimit: 64, GatewaySSEMaxSubscribers: 128, GatewayProtocolTimeout: 2 * time.Minute,
	}
	var err error
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	if config.GatewayAuthSecretVersion, err = envInt64(getenv, "TRPC_GATEWAY_AUTH_SECRET_VERSION", 0); err != nil || config.GatewayAuthSecretVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_AUTH_SECRET_VERSION")
	}
	if config.GatewayMaxBody, err = envInt64(getenv, "TRPC_GATEWAY_MAX_BODY", config.GatewayMaxBody); err != nil || config.GatewayMaxBody < 1 || config.GatewayMaxBody > 16<<20 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_MAX_BODY")
	}
	if config.GatewaySSEReplayLimit, err = envInt64(getenv, "TRPC_GATEWAY_SSE_REPLAY_LIMIT", config.GatewaySSEReplayLimit); err != nil || config.GatewaySSEReplayLimit < 1 || config.GatewaySSEReplayLimit > 256 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_SSE_REPLAY_LIMIT")
	}
	if config.GatewaySSEMaxSubscribers, err = envInt64(getenv, "TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS", config.GatewaySSEMaxSubscribers); err != nil || config.GatewaySSEMaxSubscribers < 1 || config.GatewaySSEMaxSubscribers > 10000 {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_SSE_MAX_SUBSCRIBERS")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_GATEWAY_AUTH_CLOCK_SKEW", &config.GatewayAuthClockSkew, time.Second},
		{"TRPC_GATEWAY_SSE_POLL_INTERVAL", &config.GatewaySSEPollInterval, time.Millisecond},
		{"TRPC_GATEWAY_PROTOCOL_TIMEOUT", &config.GatewayProtocolTimeout, time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.SecretRoot == "" || config.PayloadKeyRef == "" ||
		config.GatewayProbeTenant == "" || config.GatewayAuthSecretRef == "" || config.GatewayPublicURL == "" {
		return productionConfig{}, errors.New("required gateway dependency configuration is missing")
	}
	if config.GatewayAuthClockSkew >= config.ShutdownTimeout {
		return productionConfig{}, errors.New("invalid gateway lifecycle timing")
	}
	if config.GatewayProtocolTimeout > 30*time.Minute {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_PROTOCOL_TIMEOUT")
	}
	publicURL, parseErr := url.Parse(config.GatewayPublicURL)
	if parseErr != nil || publicURL.Scheme != "https" || publicURL.Host == "" || publicURL.User != nil ||
		(publicURL.Path != "" && publicURL.Path != "/") || publicURL.RawQuery != "" || publicURL.Fragment != "" {
		return productionConfig{}, errors.New("invalid TRPC_GATEWAY_PUBLIC_BASE_URL")
	}
	return config, nil
}

func loadWorkerConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:   strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), RedisAddress: strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword: getenv("TRPC_REDIS_PASSWORD"), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		RedisEnvironment: strings.TrimSpace(getenv("TRPC_REDIS_ENVIRONMENT")), PayloadKeyRef: strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")),
		S3Region: strings.TrimSpace(getenv("TRPC_S3_REGION")), S3Bucket: strings.TrimSpace(getenv("TRPC_S3_BUCKET")), S3Endpoint: strings.TrimSpace(getenv("TRPC_S3_ENDPOINT")),
		WorkerID: strings.TrimSpace(getenv("TRPC_WORKER_ID")), WorkerGroup: strings.TrimSpace(getenv("TRPC_WORKER_GROUP")),
		WorkerControlGroup: strings.TrimSpace(getenv("TRPC_WORKER_CONTROL_GROUP")), WorkerProbeTenant: strings.TrimSpace(getenv("TRPC_WORKER_PROBE_TENANT_ID")),
		ProbeTimeout: 5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 45 * time.Second,
		S3MaxBytes: 16 << 20, WorkerShardCount: 1, WorkerReclaimLimit: 100,
		WorkerLeaseTTL: 30 * time.Second, WorkerLeaseRenew: 10 * time.Second, WorkerRetryWait: 100 * time.Millisecond,
		WorkerReclaimInterval: 5 * time.Second, WorkerCancelPoll: 100 * time.Millisecond, WorkerDrainTimeout: 30 * time.Second,
		WorkerBundleFailureBackoff: 250 * time.Millisecond, WorkerBundleCloseTimeout: 5 * time.Second,
	}
	config.DLPEndpoint = strings.TrimSpace(getenv("TRPC_DLP_ENDPOINT"))
	config.DLPProbeTenant = strings.TrimSpace(getenv("TRPC_DLP_PROBE_TENANT_ID"))
	config.DLPSecretRef = strings.TrimSpace(getenv("TRPC_DLP_SECRET_REF"))
	var err error
	if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
		return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	if config.DLPEndpoint != "" || config.DLPProbeTenant != "" || config.DLPSecretRef != "" {
		if config.DLPEndpoint == "" || config.DLPProbeTenant == "" || config.DLPSecretRef == "" {
			return productionConfig{}, errors.New("incomplete worker DLP configuration")
		}
		if config.DLPSecretVersion, err = envInt64(getenv, "TRPC_DLP_SECRET_VERSION", 0); err != nil || config.DLPSecretVersion < 1 {
			return productionConfig{}, errors.New("invalid TRPC_DLP_SECRET_VERSION")
		}
		if config.DLPBackendVersion, err = envInt64(getenv, "TRPC_DLP_BACKEND_VERSION", 0); err != nil || config.DLPBackendVersion < 1 {
			return productionConfig{}, errors.New("invalid TRPC_DLP_BACKEND_VERSION")
		}
		if config.DLPAllowInsecure, err = envBool(getenv, "TRPC_DLP_ALLOW_INSECURE", false); err != nil {
			return productionConfig{}, errors.New("invalid TRPC_DLP_ALLOW_INSECURE")
		}
	}
	if config.WorkerShardCount, err = envInt(getenv, "TRPC_WORKER_SHARD_COUNT", config.WorkerShardCount); err != nil || config.WorkerShardCount < 1 || config.WorkerShardCount > 4096 {
		return productionConfig{}, errors.New("invalid TRPC_WORKER_SHARD_COUNT")
	}
	if raw := strings.TrimSpace(getenv("TRPC_WORKER_SHARDS")); raw != "" {
		config.WorkerShards, err = parseWorkerShards(raw, config.WorkerShardCount)
		if err != nil {
			return productionConfig{}, errors.New("invalid TRPC_WORKER_SHARDS")
		}
	} else {
		config.WorkerShards = make([]uint32, config.WorkerShardCount)
		for index := range config.WorkerShards {
			config.WorkerShards[index] = uint32(index)
		}
	}
	if config.WorkerReclaimLimit, err = envInt(getenv, "TRPC_WORKER_RECLAIM_LIMIT", config.WorkerReclaimLimit); err != nil || config.WorkerReclaimLimit < 1 || config.WorkerReclaimLimit > 1000 {
		return productionConfig{}, errors.New("invalid TRPC_WORKER_RECLAIM_LIMIT")
	}
	if config.S3MaxBytes, err = envInt64(getenv, "TRPC_ARTIFACT_MAX_BYTES", config.S3MaxBytes); err != nil || config.S3MaxBytes < 1 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_MAX_BYTES")
	}
	for name, target := range map[string]*bool{"TRPC_S3_PATH_STYLE": &config.S3PathStyle, "TRPC_S3_ALLOW_INSECURE": &config.S3AllowInsecure} {
		if *target, err = envBool(getenv, name, false); err != nil {
			return productionConfig{}, errors.New("invalid " + name)
		}
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond}, {"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second}, {"TRPC_WORKER_LEASE_TTL", &config.WorkerLeaseTTL, time.Second},
		{"TRPC_WORKER_LEASE_RENEW", &config.WorkerLeaseRenew, time.Millisecond}, {"TRPC_WORKER_RETRY_WAIT", &config.WorkerRetryWait, time.Millisecond},
		{"TRPC_WORKER_RECLAIM_INTERVAL", &config.WorkerReclaimInterval, time.Millisecond}, {"TRPC_WORKER_CANCEL_POLL", &config.WorkerCancelPoll, time.Millisecond},
		{"TRPC_WORKER_DRAIN_TIMEOUT", &config.WorkerDrainTimeout, time.Second}, {"TRPC_WORKER_BUNDLE_FAILURE_BACKOFF", &config.WorkerBundleFailureBackoff, time.Millisecond},
		{"TRPC_WORKER_BUNDLE_CLOSE_TIMEOUT", &config.WorkerBundleCloseTimeout, time.Millisecond},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.WorkerLeaseRenew >= config.WorkerLeaseTTL || config.WorkerDrainTimeout+config.WorkerBundleCloseTimeout+workerHTTPShutdownBudget > config.ShutdownTimeout {
		return productionConfig{}, errors.New("invalid worker lifecycle timing")
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.RedisAddress == "" || config.SecretRoot == "" ||
		config.RedisEnvironment == "" || config.WorkerGroup == "" || config.WorkerControlGroup == "" || config.WorkerProbeTenant == "" ||
		config.PayloadKeyRef == "" || config.S3Region == "" || config.S3Bucket == "" {
		return productionConfig{}, errors.New("required worker dependency configuration is missing")
	}
	return config, nil
}

func parseWorkerShards(raw string, count int) ([]uint32, error) {
	seen := make(map[uint32]struct{})
	result := make([]uint32, 0)
	for _, part := range strings.Split(raw, ",") {
		value, err := strconv.ParseUint(strings.TrimSpace(part), 10, 32)
		if err != nil || value >= uint64(count) {
			return nil, errors.New("invalid shard")
		}
		shard := uint32(value)
		if _, exists := seen[shard]; exists {
			return nil, errors.New("duplicate shard")
		}
		seen[shard] = struct{}{}
		result = append(result, shard)
	}
	if len(result) == 0 {
		return nil, errors.New("empty shards")
	}
	return result, nil
}

func loadProductionConfig(getenv func(string) string) (productionConfig, error) {
	return loadRoleConfig(getenv, true, false)
}

func loadPreprocessConfig(getenv func(string) string) (productionConfig, error) {
	config, err := loadRoleConfig(getenv, false, true)
	if err != nil {
		return productionConfig{}, err
	}
	config.ArtifactRetention, err = envDuration(getenv, "TRPC_PREPROCESS_ARTIFACT_RETENTION", 0)
	if err != nil {
		return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_ARTIFACT_RETENTION")
	}
	if config.ArtifactRetention < time.Second || config.ArtifactRetention%time.Second != 0 {
		return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_ARTIFACT_RETENTION")
	}
	return config, nil
}

func loadChannelConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		ChannelProbeTenant: strings.TrimSpace(getenv("TRPC_CHANNEL_PROBE_TENANT_ID")),
		ProbeTimeout:       5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 30 * time.Second,
		ChannelCandidateTTL: 30 * time.Second, ChannelCallbackMaxBody: 1 << 20}
	var err error
	if config.WebUIEnabled, err = envBool(getenv, "TRPC_WEBUI_ENABLED", false); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_WEBUI_ENABLED")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	config.ProbeTimeout, err = envDuration(getenv, "TRPC_PROBE_TIMEOUT", config.ProbeTimeout)
	if err != nil || config.ProbeTimeout < time.Millisecond {
		return productionConfig{}, errors.New("invalid TRPC_PROBE_TIMEOUT")
	}
	config.ProbeInterval, err = envDuration(getenv, "TRPC_PROBE_INTERVAL", config.ProbeInterval)
	if err != nil || config.ProbeInterval < time.Millisecond {
		return productionConfig{}, errors.New("invalid TRPC_PROBE_INTERVAL")
	}
	config.ShutdownTimeout, err = envDuration(getenv, "TRPC_SHUTDOWN_TIMEOUT", config.ShutdownTimeout)
	if err != nil || config.ShutdownTimeout < time.Second {
		return productionConfig{}, errors.New("invalid TRPC_SHUTDOWN_TIMEOUT")
	}
	config.ChannelCandidateTTL, err = envDuration(getenv, "TRPC_CHANNEL_CANDIDATE_TTL", config.ChannelCandidateTTL)
	if err != nil || config.ChannelCandidateTTL < time.Second || config.ChannelCandidateTTL > 10*time.Minute {
		return productionConfig{}, errors.New("invalid TRPC_CHANNEL_CANDIDATE_TTL")
	}
	if config.ChannelCallbackMaxBody, err = envInt64(getenv, "TRPC_CHANNEL_CALLBACK_MAX_BODY", config.ChannelCallbackMaxBody); err != nil ||
		config.ChannelCallbackMaxBody < 1 || config.ChannelCallbackMaxBody > 16<<20 {
		return productionConfig{}, errors.New("invalid TRPC_CHANNEL_CALLBACK_MAX_BODY")
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.SecretRoot == "" || config.ChannelProbeTenant == "" || strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")) == "" {
		return productionConfig{}, errors.New("required channel dependency configuration is missing")
	}
	config.PayloadKeyRef = strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF"))
	return config, nil
}

func loadChannelDeliveryConfig(getenv func(string) string) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")), RedisAddress: strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword: getenv("TRPC_REDIS_PASSWORD"), SecretRoot: strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		RedisEnvironment: strings.TrimSpace(getenv("TRPC_REDIS_ENVIRONMENT")), ChannelDeliveryGroup: strings.TrimSpace(getenv("TRPC_CHANNEL_DELIVERY_GROUP")),
		ChannelProbeTenant: strings.TrimSpace(getenv("TRPC_CHANNEL_PROBE_TENANT_ID")), PayloadKeyRef: strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")),
		ProbeTimeout: 5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 30 * time.Second,
		ChannelDeliveryRefresh: 15 * time.Second, ChannelReplyReadBlock: time.Second, ChannelReplyReclaimIdle: 30 * time.Second,
		ChannelReplyReclaimInterval: 5 * time.Second, ChannelDeliveryClaimTTL: 30 * time.Second, ChannelDeliveryClaimRenew: 10 * time.Second,
		ChannelDeliveryRetryDelay: time.Second, ChannelDeliveryMaxRetry: time.Minute,
		ChannelProviderTimeout:   15 * time.Second,
		ChannelReplyReclaimLimit: 100, ChannelDeliveryMaxAttempts: 8, ChannelDeliveryMaxReconcile: 8}
	var err error
	if config.WebUIEnabled, err = envBool(getenv, "TRPC_WEBUI_ENABLED", false); err != nil {
		return productionConfig{}, errors.New("invalid TRPC_WEBUI_ENABLED")
	}
	if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
		return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	for _, item := range []struct {
		name    string
		target  *time.Duration
		minimum time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond}, {"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second}, {"TRPC_CHANNEL_DELIVERY_REFRESH", &config.ChannelDeliveryRefresh, time.Millisecond},
		{"TRPC_CHANNEL_REPLY_READ_BLOCK", &config.ChannelReplyReadBlock, time.Millisecond}, {"TRPC_CHANNEL_REPLY_RECLAIM_IDLE", &config.ChannelReplyReclaimIdle, time.Millisecond},
		{"TRPC_CHANNEL_REPLY_RECLAIM_INTERVAL", &config.ChannelReplyReclaimInterval, time.Millisecond}, {"TRPC_CHANNEL_DELIVERY_CLAIM_TTL", &config.ChannelDeliveryClaimTTL, time.Millisecond},
		{"TRPC_CHANNEL_DELIVERY_CLAIM_RENEW", &config.ChannelDeliveryClaimRenew, time.Millisecond}, {"TRPC_CHANNEL_DELIVERY_RETRY_DELAY", &config.ChannelDeliveryRetryDelay, time.Millisecond},
		{"TRPC_CHANNEL_DELIVERY_MAX_RETRY", &config.ChannelDeliveryMaxRetry, time.Millisecond},
		{"TRPC_CHANNEL_PROVIDER_TIMEOUT", &config.ChannelProviderTimeout, time.Millisecond},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	for _, item := range []struct {
		name   string
		target *int
	}{
		{"TRPC_CHANNEL_REPLY_RECLAIM_LIMIT", &config.ChannelReplyReclaimLimit}, {"TRPC_CHANNEL_DELIVERY_MAX_ATTEMPTS", &config.ChannelDeliveryMaxAttempts},
		{"TRPC_CHANNEL_DELIVERY_MAX_RECONCILE", &config.ChannelDeliveryMaxReconcile},
	} {
		if *item.target, err = envInt(getenv, item.name, *item.target); err != nil || *item.target < 1 || *item.target > 1000 {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ChannelDeliveryClaimRenew >= config.ChannelDeliveryClaimTTL {
		return productionConfig{}, errors.New("channel delivery claim renew must be shorter than claim TTL")
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.RedisAddress == "" || config.SecretRoot == "" || config.RedisEnvironment == "" ||
		config.ChannelDeliveryGroup == "" || config.ChannelProbeTenant == "" || config.PayloadKeyRef == "" {
		return productionConfig{}, errors.New("required channel delivery dependency configuration is missing")
	}
	return config, nil
}

func loadRoleConfig(getenv func(string) string, requireRedis, requirePreprocess bool) (productionConfig, error) {
	if getenv == nil {
		return productionConfig{}, errors.New("environment reader is required")
	}
	config := productionConfig{
		ListenAddress:          valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:            strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		RedisAddress:           strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")),
		RedisPassword:          getenv("TRPC_REDIS_PASSWORD"),
		SecretRoot:             strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		S3Region:               strings.TrimSpace(getenv("TRPC_S3_REGION")),
		S3Bucket:               strings.TrimSpace(getenv("TRPC_S3_BUCKET")),
		S3Endpoint:             strings.TrimSpace(getenv("TRPC_S3_ENDPOINT")),
		ClamAVAddress:          strings.TrimSpace(getenv("TRPC_CLAMAV_ADDRESS")),
		DLPEndpoint:            strings.TrimSpace(getenv("TRPC_DLP_ENDPOINT")),
		DLPProbeTenant:         strings.TrimSpace(getenv("TRPC_DLP_PROBE_TENANT_ID")),
		DLPSecretRef:           strings.TrimSpace(getenv("TRPC_DLP_SECRET_REF")),
		PayloadKeyRef:          strings.TrimSpace(getenv("TRPC_PAYLOAD_KEY_REF")),
		ProbeTimeout:           5 * time.Second,
		ProbeInterval:          15 * time.Second,
		ShutdownTimeout:        30 * time.Second,
		ArtifactPutTimeout:     30 * time.Second,
		UploadProtection:       2 * time.Minute,
		UploadClaimTTL:         time.Minute,
		ArtifactClaimTTL:       time.Minute,
		UploadPollInterval:     time.Minute,
		ArtifactPollInterval:   time.Minute,
		ArtifactOrphanGrace:    24 * time.Hour,
		LifecycleBatchSize:     100,
		LifecycleMaxAttempts:   8,
		S3MaxBytes:             16 << 20,
		PreprocessBatchSize:    100,
		PreprocessMaxAttempts:  8,
		PreprocessLeaseTTL:     30 * time.Second,
		PreprocessRetryDelay:   time.Second,
		PreprocessPollInterval: time.Second,
		MediaFetchTimeout:      15 * time.Second,
	}
	var err error
	if requireRedis {
		if config.RedisDB, err = envInt(getenv, "TRPC_REDIS_DB", 0); err != nil || config.RedisDB < 0 {
			return productionConfig{}, errors.New("invalid TRPC_REDIS_DB")
		}
	}
	if config.DLPSecretVersion, err = envInt64(getenv, "TRPC_DLP_SECRET_VERSION", 0); err != nil || config.DLPSecretVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_DLP_SECRET_VERSION")
	}
	if config.DLPBackendVersion, err = envInt64(getenv, "TRPC_DLP_BACKEND_VERSION", 0); err != nil || config.DLPBackendVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_DLP_BACKEND_VERSION")
	}
	if config.PayloadKeyVersion, err = envInt64(getenv, "TRPC_PAYLOAD_KEY_VERSION", 0); err != nil || config.PayloadKeyVersion < 1 {
		return productionConfig{}, errors.New("invalid TRPC_PAYLOAD_KEY_VERSION")
	}
	for name, target := range map[string]*bool{
		"TRPC_S3_PATH_STYLE": &config.S3PathStyle, "TRPC_S3_ALLOW_INSECURE": &config.S3AllowInsecure,
		"TRPC_DLP_ALLOW_INSECURE": &config.DLPAllowInsecure,
	} {
		if *target, err = envBool(getenv, name, false); err != nil {
			return productionConfig{}, errors.New("invalid " + name)
		}
	}
	if requirePreprocess {
		config.MediaAllowedHosts = envCSV(getenv, "TRPC_PREPROCESS_MEDIA_ALLOWED_HOSTS")
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
	if requirePreprocess {
		durations = append(durations,
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_LEASE_TTL", &config.PreprocessLeaseTTL, time.Second},
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_RETRY_DELAY", &config.PreprocessRetryDelay, time.Millisecond},
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_POLL_INTERVAL", &config.PreprocessPollInterval, time.Millisecond},
			struct {
				name    string
				target  *time.Duration
				minimum time.Duration
			}{"TRPC_PREPROCESS_MEDIA_FETCH_TIMEOUT", &config.MediaFetchTimeout, time.Millisecond},
		)
	}
	for _, item := range durations {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.minimum {
			return productionConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.LifecycleBatchSize, err = envInt(getenv, "TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE", config.LifecycleBatchSize); err != nil || config.LifecycleBatchSize < 1 || config.LifecycleBatchSize > 1000 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_LIFECYCLE_BATCH_SIZE")
	}
	if requirePreprocess {
		if config.PreprocessBatchSize, err = envInt(getenv, "TRPC_PREPROCESS_BATCH_SIZE", config.PreprocessBatchSize); err != nil || config.PreprocessBatchSize < 1 || config.PreprocessBatchSize > 1000 {
			return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_BATCH_SIZE")
		}
		if config.PreprocessMaxAttempts, err = envInt(getenv, "TRPC_PREPROCESS_MAX_ATTEMPTS", config.PreprocessMaxAttempts); err != nil || config.PreprocessMaxAttempts < 1 || config.PreprocessMaxAttempts > 100 {
			return productionConfig{}, errors.New("invalid TRPC_PREPROCESS_MAX_ATTEMPTS")
		}
	}
	if config.LifecycleMaxAttempts, err = envInt(getenv, "TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS", config.LifecycleMaxAttempts); err != nil || config.LifecycleMaxAttempts < 1 || config.LifecycleMaxAttempts > 100 {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_LIFECYCLE_MAX_ATTEMPTS")
	}
	maxBytes, err := envInt64(getenv, "TRPC_ARTIFACT_MAX_BYTES", config.S3MaxBytes)
	if err != nil || maxBytes < 1 || uint64(maxBytes) > uint64(^uint(0)>>1) {
		return productionConfig{}, errors.New("invalid TRPC_ARTIFACT_MAX_BYTES")
	}
	config.S3MaxBytes = maxBytes
	if config.ListenAddress == "" || config.PostgresDSN == "" || (requireRedis && config.RedisAddress == "") || config.SecretRoot == "" || config.S3Region == "" ||
		config.S3Bucket == "" || config.ClamAVAddress == "" ||
		config.DLPEndpoint == "" || config.DLPProbeTenant == "" || config.DLPSecretRef == "" || config.PayloadKeyRef == "" {
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

func envCSV(getenv func(string) string, name string) []string {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		result = append(result, part)
	}
	return result
}
