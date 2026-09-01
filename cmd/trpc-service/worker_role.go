package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	checkpointredis "github.com/liuzengh/trpc-agent-service/trpcservice/agent/checkpointredis"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	brokerredis "github.com/liuzengh/trpc-agent-service/trpcservice/broker/redis"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	coordinationredis "github.com/liuzengh/trpc-agent-service/trpcservice/coordination/redis"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/governance/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/httpdlp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilecontrol "github.com/liuzengh/trpc-agent-service/trpcservice/profile/controlplane"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider/modelclient"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	relayredis "github.com/liuzengh/trpc-agent-service/trpcservice/relay/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	artifactpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/postgres"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	objectstores3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func runWorkerRole(parent context.Context, getenv func(string) string, logger *log.Logger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadWorkerConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "worker", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(32)

	redis := redisclient.NewClient(&redisclient.Options{Addr: configValue.RedisAddress, Password: configValue.RedisPassword, DB: configValue.RedisDB})
	defer redis.Close()
	awsValue, err := awsconfig.LoadDefaultConfig(parent, awsconfig.WithRegion(configValue.S3Region))
	if err != nil {
		return errors.New("AWS SDK configuration failed")
	}
	objects, err := objectstores3.NewFromConfig(awsValue, configValue.S3Bucket, configValue.S3Endpoint, configValue.S3PathStyle,
		objectstores3.Options{MaxBytes: configValue.S3MaxBytes, AllowInsecure: configValue.S3AllowInsecure})
	if err != nil {
		return errors.New("object store configuration rejected")
	}
	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, configValue.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	catalog, err := provider.NewCatalog(provider.DeepSeekModelSchema())
	if err != nil {
		return errors.New("provider catalog initialization failed")
	}
	tenantRepo := tenantpostgres.New(db)
	agentRepo := agentpostgres.New(db)
	configRepo := configpostgres.New(db, tenantRepo)
	providerRepo := providerpostgres.New(db, catalog)
	profiles := profilecontrol.Resolver{Tenants: tenantRepo, Agents: agentRepo, Configs: configRepo, Models: providerRepo}
	models := modelclient.Resolver{Profiles: providerRepo, Secrets: secretProvider, Subject: "worker-model"}
	toolCatalog, err := servicetool.NewCatalog()
	if err != nil {
		return errors.New("tool catalog initialization failed")
	}
	tools := servicetool.Resolver{Catalog: toolCatalog, Secrets: secretProvider}
	governanceStore := governancepostgres.New(db)
	graphCheckpoints := checkpointredis.Resolver{Client: redis, TTL: configValue.WorkerGraphCheckpointTTL}
	agentFactory := serviceagent.Factory{Profiles: profiles, Models: models, Tools: tools, Checkpoints: graphCheckpoints,
		Policies: governanceStore, Telemetry: telemetryProvider}
	bundles := profilememory.NewBundleManagerWithPolicy(func(ctx context.Context, key profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		snapshot, resolveErr := profiles.Resolve(ctx, key)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		root, buildErr := agentFactory.Build(ctx, snapshot)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		return &serviceagent.Bundle{AppName: snapshot.AppName, Root: root}, nil, nil
	}, profilememory.BundleManagerPolicy{FailureBackoff: configValue.WorkerBundleFailureBackoff, CloseTimeout: configValue.WorkerBundleCloseTimeout})

	tasks := gatewaypostgres.NewTaskStore(db)
	runGovernance := governance.Service{Repository: governanceStore, Ledger: governanceStore, Decisions: governanceStore}
	var dlpScanner *httpdlp.Scanner
	if configValue.DLPEndpoint != "" {
		authorizer, authErr := newDLPAuthorizer(secretProvider, configValue.DLPBackendVersion, secrets.SecretRef{Ref: configValue.DLPSecretRef, Version: configValue.DLPSecretVersion})
		if authErr != nil {
			return errors.New("worker DLP authorization configuration rejected")
		}
		scanner := httpdlp.Scanner{Endpoint: configValue.DLPEndpoint, Authorize: authorizer, ProbeTenantID: configValue.DLPProbeTenant,
			Timeout: configValue.ProbeTimeout, MaxBytes: 16 << 20, AllowInsecure: configValue.DLPAllowInsecure}
		dlpScanner = &scanner
		guard := governance.ScannerContentGuard{Scanner: scanner}
		runGovernance.InputGuard, runGovernance.OutputGuard = guard, guard
	}
	sessions := sessionpostgres.New(db)
	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, payloadKeys)
	agentFactory.Confirmations, agentFactory.ToolResults = governanceStore, payloads
	artifacts := artifactpostgres.NewWithObjectStore(db, objects)
	executor := worker.RunnerExecutor{Tasks: tasks, Profiles: profiles, Bundles: bundles, Sessions: sessions,
		Payloads: payloads, Artifacts: artifacts, Inputs: worker.JSONTextInputDecoder{}, EncodeEvent: worker.DurableEventRef,
		EventDrainTimeout: configValue.WorkerBundleCloseTimeout, Governance: runGovernance, Confirmations: governanceStore,
		ContinuationTools: agentFactory, Telemetry: telemetryProvider}
	dispatchBroker, err := brokerredis.New(redis, brokerredis.Config{Environment: configValue.RedisEnvironment, Group: configValue.WorkerGroup,
		ShardCount: uint32(configValue.WorkerShardCount), ReadBlock: 250 * time.Millisecond, ReclaimIdle: configValue.WorkerLeaseTTL})
	if err != nil {
		return errors.New("execution broker configuration rejected")
	}
	brokerMetrics := &metrics.BrokerRegistry{SnapshotTTL: 3 * configValue.WorkerBacklogPoll}
	backlogMonitor := broker.BacklogMonitor{Source: dispatchBroker, Observer: brokerMetrics, PollInterval: configValue.WorkerBacklogPoll}
	leases, err := coordinationredis.New(redis, configValue.RedisEnvironment)
	if err != nil {
		return errors.New("lease manager configuration rejected")
	}
	workerID := configValue.WorkerID
	if workerID == "" {
		host, _ := os.Hostname()
		workerID = fmt.Sprintf("%s-%d", valueOr(host, "trpc-worker"), os.Getpid())
	}
	shards := make([]broker.Shard, len(configValue.WorkerShards))
	for index, shard := range configValue.WorkerShards {
		shards[index] = broker.Shard(shard)
	}
	hints := &worker.CancelHintHub{}
	publisher, err := relayredis.NewPublisher(redis, relayredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("execution control publisher configuration rejected")
	}
	controlQueue, err := relayredis.NewExecutionControlQueue(redis, publisher, relayredis.ExecutionControlQueueConfig{
		Group: configValue.WorkerControlGroup, ReadBlock: 250 * time.Millisecond, ReclaimIdle: configValue.WorkerLeaseTTL})
	if err != nil {
		return errors.New("execution control queue configuration rejected")
	}
	lifecycle := worker.NewLifecycle()
	consumer := worker.Consumer{WorkerID: workerID, Shards: shards, Broker: dispatchBroker, Leases: leases, Sessions: sessions,
		Parker: tasks, Statuses: tasks, Executor: executor, LeaseTTL: configValue.WorkerLeaseTTL, RenewInterval: configValue.WorkerLeaseRenew,
		RetryWait: configValue.WorkerRetryWait, ReclaimInterval: configValue.WorkerReclaimInterval, ReclaimLimit: configValue.WorkerReclaimLimit,
		CancelPollInterval: configValue.WorkerCancelPoll, CancelHints: hints, DrainTimeout: configValue.WorkerDrainTimeout, Lifecycle: lifecycle,
		OnDeliveryError: func(_ context.Context, delivery broker.Delivery, deliveryErr error) {
			logger.Printf("worker delivery degraded tenant=%q request=%q: %v", delivery.Envelope.TenantID, delivery.Envelope.RequestID, deliveryErr)
		}}

	migrationReadiness := migrations.NewRunner(db)
	dependencies := []health.Dependency{
		{Name: "postgres", Probe: db.PingContext}, {Name: "postgres_schema", Probe: migrationReadiness.Ready},
		{Name: "redis", Probe: func(ctx context.Context) error { return redis.Ping(ctx).Err() }},
		{Name: "object_store", Probe: objects.Probe}, {Name: "secret_provider", Probe: secretProvider.ProbeRoot},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, resolveErr := payloadKeys.ResolvePayloadKey(ctx, configValue.WorkerProbeTenant, configValue.PayloadKeyVersion)
			clear(value.Bytes)
			return resolveErr
		}},
	}
	if dlpScanner != nil {
		dependencies = append(dependencies, health.Dependency{Name: "governance_dlp", Probe: dlpScanner.Probe})
	}
	monitor, err := health.NewMonitor(lifecycle, dependencies, configValue.ProbeTimeout, configValue.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}
	listener, err := net.Listen("tcp", configValue.ListenAddress)
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	mux.Handle("/metrics", brokerMetrics)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 4)
	consumerDone := make(chan error, 1)
	var background sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if operationErr := operation(processCtx); operationErr != nil && !errors.Is(operationErr, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	start("broker backlog monitor", backlogMonitor.Run)
	start("execution control consumer", func(ctx context.Context) error {
		return controlQueue.ConsumeExecutionControl(ctx, relay.ExecutionControlConsumerOptions{ConsumerID: workerID + "-control"}, hints.ConsumeExecutionControl)
	})
	start("execution control reclaimer", func(ctx context.Context) error {
		ticker := time.NewTicker(configValue.WorkerReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				deliveries, reclaimErr := controlQueue.ReclaimExecutionControls(ctx, relay.ExecutionControlConsumerOptions{ConsumerID: workerID + "-control", Limit: configValue.WorkerReclaimLimit})
				if reclaimErr != nil {
					logger.Printf("worker execution-control reclaim degraded: %v", reclaimErr)
					continue
				}
				for _, delivery := range deliveries {
					if handleErr := hints.ConsumeExecutionControl(ctx, delivery); handleErr != nil {
						logger.Printf("worker execution-control hint rejected: %v", handleErr)
						continue
					}
					if ackErr := controlQueue.AckExecutionControl(ctx, delivery); ackErr != nil {
						logger.Printf("worker execution-control reclaim ACK degraded: %v", ackErr)
					}
				}
			}
		}
	})
	start("confirmation expiry reconciler", func(ctx context.Context) error {
		reconciler := governance.ConfirmationExpiryReconciler{Coordinator: governanceStore, BatchSize: configValue.WorkerReclaimLimit}
		ticker := time.NewTicker(configValue.WorkerReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if _, expiryErr := reconciler.RunOnce(ctx); expiryErr != nil {
					logger.Printf("worker confirmation expiry degraded: %v", expiryErr)
				}
			}
		}
	})
	background.Add(1)
	go func() {
		defer background.Done()
		serveErr := server.Serve(listener)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	go func() {
		consumerDone <- consumer.Run(processCtx)
	}()
	logger.Printf("trpc-agent-service worker id=%q shards=%v lifecycle/readiness listening on %s", workerID, configValue.WorkerShards, configValue.ListenAddress)

	var terminalErr error
	consumerFinished := false
	select {
	case <-parent.Done():
		lifecycle.BeginDrain()
	case <-lifecycle.Drain():
	case consumerErr := <-consumerDone:
		consumerFinished = true
		if consumerErr != nil && !errors.Is(consumerErr, context.Canceled) {
			terminalErr = errors.New("worker consumer stopped")
		}
		lifecycle.BeginDrain()
	case terminalErr = <-errorsCh:
		lifecycle.BeginDrain()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), configValue.ShutdownTimeout)
	defer cancelShutdown()
	if !consumerFinished {
		select {
		case consumerErr := <-consumerDone:
			consumerFinished = true
			if consumerErr != nil && !errors.Is(consumerErr, context.Canceled) && terminalErr == nil {
				terminalErr = errors.New("worker consumer stopped")
			}
		case <-shutdownCtx.Done():
			if terminalErr == nil {
				terminalErr = errors.New("worker drain timed out")
			}
		}
	}
	cancelProcess()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && terminalErr == nil {
		terminalErr = errors.New("HTTP shutdown timed out")
	}
	if closeErr := bundles.Close(shutdownCtx); closeErr != nil && terminalErr == nil {
		terminalErr = errors.New("runtime bundle shutdown timed out")
	}
	backgroundDone := make(chan struct{})
	go func() { background.Wait(); close(backgroundDone) }()
	select {
	case <-backgroundDone:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("background shutdown timed out")
		}
	}
	lifecycle.MarkStopped()
	return terminalErr
}
