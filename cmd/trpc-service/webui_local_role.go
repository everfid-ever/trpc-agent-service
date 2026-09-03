package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	checkpointredis "github.com/liuzengh/trpc-agent-service/trpcservice/agent/checkpointredis"
	agentapp "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	brokerredis "github.com/liuzengh/trpc-agent-service/trpcservice/broker/redis"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	channeldelivery "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery"
	deliverypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingresspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
	webuipostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui/postgres"
	configdomain "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	coordinationredis "github.com/liuzengh/trpc-agent-service/trpcservice/coordination/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/governance/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/postgres"
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
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/generation"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	artifactpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/postgres"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/localnote"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const (
	webUILocalTenantID    = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	webUILocalAppID       = "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	webUILocalChildAppID  = "app_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	webUILocalBindingID   = "local-webui"
	webUILocalAccountID   = "local-webui"
	webUILocalModelID     = "deepseek-local"
	webUILocalRouteKey    = "local-webui"
	webUILocalToken       = "local-webui-token-change-me"
	payloadKeyRef         = "secret://local/payload-key"
	webUILocalInstruction = "You are a concise and helpful assistant. When the user asks to create, save, or record a note, call webui_create_note. Never claim that a note was created before the tool result is available."
)

type webUILocalConfig struct {
	PostgresDSN, RedisAddress, ListenAddress string
	RedisEnvironment, SecretRoot, APIKeyFile string
	RouteKey, Token                          string
}

type webUILocalBootstrap struct {
	Tenant       tenant.Tenant
	Config       configdomain.Snapshot
	Route        ingress.BindingRoute
	SecretRoot   string
	PayloadKey   *payloadkey.Resolver
	SecretStore  *secretfs.Provider
	ProviderRepo *providerpostgres.Repository
}

func runWebUILocalRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || getenv == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadWebUILocalConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "webui-local", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	redis := redisclient.NewClient(&redisclient.Options{Addr: configValue.RedisAddress})
	defer redis.Close()
	if err := db.PingContext(parent); err != nil {
		return errors.New("postgres unavailable")
	}
	if err := redis.Ping(parent).Err(); err != nil {
		return errors.New("redis unavailable")
	}
	bootstrap, err := bootstrapWebUILocal(parent, db, configValue)
	if err != nil {
		return fmt.Errorf("local bootstrap failed: %w", err)
	}

	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, bootstrap.PayloadKey)
	inbox := messagingpostgres.NewWithPayloadKeyResolver(db, bootstrap.PayloadKey)
	tasks := gatewaypostgres.NewTaskStore(db)
	preprocessStore := preprocesspostgres.New(db)
	bindings := ingresspostgres.New(db)
	resolver := ingress.Resolver{Store: bindings, Secrets: bootstrap.SecretStore, TTL: 30 * time.Second}
	webuiMailbox := webuipostgres.New(db)
	webuiAdapter := &webui.Adapter{Protocol: webui.Verifier{}, Mailbox: webuiMailbox}
	endpoint, err := newChannelEndpoint(webuiAdapter, resolver, identity.Mapper{Secrets: bootstrap.SecretStore},
		preprocessStore, payloads, 1, 1<<20, telemetryProvider)
	if err != nil {
		return errors.New("WebUI callback configuration rejected")
	}
	browser := webui.BrowserHandler{Callback: endpoint, Routes: bindings, Secrets: bootstrap.SecretStore,
		Messages: webuiMailbox, Results: payloads}

	streamBroker, err := brokerredis.New(redis, brokerredis.Config{Environment: configValue.RedisEnvironment,
		Group: "webui-workers", ShardCount: 4, ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
	if err != nil {
		return errors.New("broker configuration rejected")
	}
	leases, err := coordinationredis.New(redis, configValue.RedisEnvironment)
	if err != nil {
		return errors.New("lease configuration rejected")
	}
	publisher, err := relayredis.NewPublisher(redis, relayredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("relay publisher configuration rejected")
	}

	tenantRepo := tenantpostgres.New(db)
	appRepo := agentpostgres.New(db)
	configRepo := configpostgres.New(db, tenantRepo)
	profiles := profilecontrol.Resolver{Tenants: tenantRepo, Agents: appRepo, Configs: configRepo, Models: bootstrap.ProviderRepo}
	models := modelclient.Resolver{Profiles: bootstrap.ProviderRepo, Secrets: bootstrap.SecretStore, Credentials: generation.New(bootstrap.SecretStore), Subject: "worker-model"}
	governanceStore := governancepostgres.New(db)
	toolCatalog, err := servicetool.NewCatalog(localnote.Registration(webUILocalTenantID))
	if err != nil {
		return errors.New("tool catalog initialization failed")
	}
	tools := servicetool.Resolver{Catalog: toolCatalog, Secrets: bootstrap.SecretStore}
	browser.ReplyRoutes = payloads
	browser.Confirmations = governanceStore
	browser.Actions = governance.ConfirmationActionService{Coordinator: governanceStore}
	graphCheckpoints := checkpointredis.Resolver{Client: redis, TTL: 7 * 24 * time.Hour}
	agentFactory := serviceagent.Factory{Profiles: profiles, Models: models, Tools: tools, Checkpoints: graphCheckpoints,
		Policies: governanceStore, Confirmations: governanceStore, ToolResults: payloads, Telemetry: telemetryProvider}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, key profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		snapshot, resolveErr := profiles.Resolve(ctx, key)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		root, buildErr := agentFactory.Build(ctx, snapshot)
		if buildErr != nil {
			return nil, nil, buildErr
		}
		return &serviceagent.Bundle{AppName: snapshot.AppName, Root: root}, nil, nil
	})
	defer bundles.Close(context.Background())
	executor := worker.RunnerExecutor{Tasks: tasks, Profiles: profiles, Bundles: bundles,
		Sessions: sessionpostgres.New(db), Payloads: payloads, Artifacts: artifactpostgres.New(db),
		Inputs: worker.JSONTextInputDecoder{}, EncodeEvent: worker.DurableEventRef, EventDrainTimeout: 30 * time.Second,
		Governance: governance.Service{Repository: governanceStore, Ledger: governanceStore, Decisions: governanceStore}, Confirmations: governanceStore,
		ContinuationTools: agentFactory, Telemetry: telemetryProvider}
	workerConsumer := worker.Consumer{WorkerID: "webui-local-worker", Shards: []broker.Shard{0, 1, 2, 3}, Broker: streamBroker,
		Leases: leases, Sessions: sessionpostgres.New(db), Parker: tasks, Statuses: tasks, Executor: executor,
		LeaseTTL: 30 * time.Second, RenewInterval: 10 * time.Second, RetryWait: 250 * time.Millisecond,
		ReclaimInterval: 5 * time.Second, ReclaimLimit: 100, DrainTimeout: 30 * time.Second,
		OnDeliveryError: func(_ context.Context, delivery broker.Delivery, deliveryErr error) {
			logger.Printf("webui worker delivery degraded request=%q: %v", delivery.Envelope.RequestID, deliveryErr)
		}}

	dispatcher := gateway.BrokerDispatcher{Tasks: tasks, Bindings: configRepo}
	preprocessor := preprocess.Worker{Store: preprocessStore, Payloads: payloads, Dispatcher: dispatcher,
		Owner: "webui-local-preprocess", LeaseTTL: 30 * time.Second, RetryDelay: time.Second, MaxAttempts: 8, Telemetry: telemetryProvider}
	dispatchRelay := relay.DispatchRelay{Outbox: inbox, Tasks: tasks, Broker: streamBroker, Owner: "webui-local-dispatch-relay",
		ShardCount: 4, ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond,
		Telemetry: telemetryProvider}
	replyRelay := relay.ReplyRelay{Outbox: inbox, Results: payloads, Routes: inbox, Replies: publisher,
		Owner: "webui-local-reply-relay", ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond,
		Telemetry: telemetryProvider}
	wakeupQueue, err := relayredis.NewWakeupQueue(redis, publisher, relayredis.WakeupQueueConfig{
		Group: "webui-wakeup", ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
	if err != nil {
		return errors.New("wakeup queue configuration rejected")
	}
	wakeupRelay := relay.WakeupRelay{Outbox: inbox, Wakeups: publisher, Owner: "webui-local-wakeup-relay",
		ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond,
		Telemetry: telemetryProvider}
	wakeupDispatcher := relay.WakeupDispatcher{ConsumerID: "webui-local-wakeup", Wakeups: wakeupQueue,
		Store: tasks, Dispatch: streamBroker, ShardCount: 4, ReclaimInterval: 5 * time.Second, ReclaimLimit: 100}

	replyQueue, err := relayredis.NewReplyQueue(redis, publisher, relayredis.ReplyQueueConfig{
		Group: "webui-delivery", ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
	if err != nil {
		return errors.New("reply queue configuration rejected")
	}
	deliveryCatalog, err := deliverypostgres.New(db, webuiAdapter)
	if err != nil {
		return errors.New("delivery catalog configuration rejected")
	}
	deliveryService := channeldelivery.Service{Results: payloads, Ledger: inbox, Adapters: deliveryCatalog,
		Owner: "webui-local-delivery", ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second,
		DefaultRetryDelay: time.Second, MaxRetryDelay: time.Minute, MaxAttempts: 8, MaxReconcileAttempts: 8}
	deliverySupervisor := channeldelivery.Supervisor{Catalog: deliveryCatalog, RefreshInterval: time.Second,
		NewConsumer: func(destination channel.ReplyDestination) (channeldelivery.ConsumerRunner, error) {
			return channeldelivery.Consumer{Queue: replyQueue, Deliverer: deliveryService, Destination: destination,
				ConsumerID: "webui-local-delivery", ReclaimInterval: 5 * time.Second, ReclaimLimit: 100,
				Telemetry: telemetryProvider}, nil
		}, OnError: func(supervisorErr error) { logger.Printf("webui delivery degraded: %v", supervisorErr) }}

	mux := http.NewServeMux()
	mux.Handle("/webui", browser)
	mux.Handle("/webui/", browser)
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if db.PingContext(request.Context()) != nil || redis.Ping(request.Context()).Err() != nil {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: configValue.ListenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsCh := make(chan error, 8)
	var background sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if operationErr := operation(processCtx); operationErr != nil && !errors.Is(operationErr, context.Canceled) {
				select {
				case errorsCh <- fmt.Errorf("%s stopped: %w", name, operationErr):
				case <-processCtx.Done():
				}
			}
		}()
	}
	start("preprocess", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, preprocessor.RunOnce, 100*time.Millisecond, 100, logger)
	})
	start("confirmation expiry reconciler", func(ctx context.Context) error {
		reconciler := governance.ConfirmationExpiryReconciler{Coordinator: governanceStore, BatchSize: 100}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if _, expiryErr := reconciler.RunOnce(ctx); expiryErr != nil {
					logger.Printf("webui confirmation expiry degraded: %v", expiryErr)
				}
			}
		}
	})
	start("dispatch relay", dispatchRelay.Run)
	start("worker", workerConsumer.Run)
	start("reply relay", replyRelay.Run)
	start("wakeup relay", wakeupRelay.Run)
	start("wakeup dispatcher", wakeupDispatcher.Run)
	start("delivery", deliverySupervisor.Run)
	start("http", func(context.Context) error { return server.ListenAndServe() })
	logger.Printf("WebUI local ready: http://localhost%s/webui/ route=%q account=%q model=deepseek",
		configValue.ListenAddress, configValue.RouteKey, webUILocalAccountID)

	var terminalErr error
	select {
	case <-processCtx.Done():
	case terminalErr = <-errorsCh:
	}
	stop()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	_ = server.Shutdown(shutdownCtx)
	done := make(chan struct{})
	go func() { background.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("webui local shutdown timed out")
		}
	}
	return terminalErr
}

func loadWebUILocalConfig(getenv func(string) string) (webUILocalConfig, error) {
	value := webUILocalConfig{PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		RedisAddress: strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")), ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		RedisEnvironment: valueOr(getenv("TRPC_REDIS_ENVIRONMENT"), "m2-webui-local"),
		SecretRoot:       valueOr(getenv("TRPC_WEBUI_LOCAL_SECRET_ROOT"), "/tmp/trpc-webui-secrets"),
		APIKeyFile:       valueOr(getenv("TRPC_WEBUI_DEEPSEEK_KEY_FILE"), "/run/secrets/deepseek_api_key"),
		RouteKey:         valueOr(getenv("TRPC_WEBUI_LOCAL_ROUTE_KEY"), webUILocalRouteKey),
		Token:            valueOr(getenv("TRPC_WEBUI_LOCAL_TOKEN"), webUILocalToken)}
	if value.PostgresDSN == "" || value.RedisAddress == "" || strings.TrimSpace(value.Token) != value.Token || len(value.Token) < 16 ||
		strings.TrimSpace(value.RouteKey) != value.RouteKey || value.RouteKey == "" || !filepath.IsAbs(value.APIKeyFile) || !filepath.IsAbs(value.SecretRoot) {
		return webUILocalConfig{}, errors.New("required WebUI local configuration is missing or invalid")
	}
	return value, nil
}

func bootstrapWebUILocal(ctx context.Context, db *sql.DB, configValue webUILocalConfig) (webUILocalBootstrap, error) {
	if err := migrations.NewRunner(db).Up(ctx); err != nil {
		return webUILocalBootstrap{}, err
	}
	apiKey, err := os.ReadFile(configValue.APIKeyFile)
	if err != nil || strings.TrimSpace(string(apiKey)) == "" || strings.ContainsRune(strings.TrimSpace(string(apiKey)), '\n') {
		return webUILocalBootstrap{}, errors.New("DeepSeek API key file is missing or invalid")
	}
	apiKey = []byte(strings.TrimSpace(string(apiKey)))
	defer clear(apiKey)
	if err := os.MkdirAll(configValue.SecretRoot, 0o700); err != nil {
		return webUILocalBootstrap{}, err
	}
	if err := os.Chmod(configValue.SecretRoot, 0o700); err != nil {
		return webUILocalBootstrap{}, err
	}

	catalog, err := provider.NewCatalog(provider.DeepSeekModelSchema())
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	tenants := tenantpostgres.New(db)
	apps := agentpostgres.New(db)
	configs := configpostgres.New(db, tenants)
	providers := providerpostgres.New(db, catalog)
	governanceStore := governancepostgres.New(db)
	root, err := tenants.Get(ctx, webUILocalTenantID)
	if errors.Is(err, tenant.ErrNotFound) {
		metadata := tenant.ChangeMetadata{ActorType: "system", ActorID: "webui-local", ReasonCode: "local_bootstrap",
			CorrelationID: "webui-local", TraceID: "webui-local"}
		root, err = tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: webUILocalTenantID,
			TenantKey: "webui-local", DisplayName: "WebUI Local"}, ChangeMetadata: metadata})
		if err != nil {
			return webUILocalBootstrap{}, err
		}
		_, err = providers.PublishModel(ctx, provider.ModelProfileSnapshot{TenantID: webUILocalTenantID,
			ProfileID: webUILocalModelID, ProfileKey: "deepseek-local", DisplayName: "DeepSeek Local", Status: "active",
			SchemaVersion: 1, Provider: "deepseek", Model: "deepseek-v4-flash", Endpoint: "https://api.deepseek.com",
			SecretRef: secrets.SecretRef{Ref: "secret://local/deepseek", Version: 1}, Version: 1})
		if err != nil {
			return webUILocalBootstrap{}, err
		}
		policy := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow,
			AllowedModels: []governance.VersionedRef{{ID: webUILocalModelID, Version: 1}},
			Tools:         []governance.ToolRule{{ToolID: localnote.ID, Version: localnote.Version, Dangerous: true, ConfirmationSupported: true}},
			InputDLP:      governance.DLPDisabled, OutputDLP: governance.DLPDisabled}
		policyDigest, _, digestErr := governance.PolicyDigest(policy)
		if digestErr != nil {
			return webUILocalBootstrap{}, digestErr
		}
		if err = governanceStore.PublishPolicy(ctx, governance.PolicySnapshot{TenantID: webUILocalTenantID, Version: 1, SchemaVersion: 1,
			Policy: policy, ContentDigest: policyDigest, PublishedAt: time.Now().UTC()}); err != nil {
			return webUILocalBootstrap{}, err
		}
		appMetadata := agentapp.ChangeMetadata{ActorType: "system", ActorID: "webui-local", Reason: "local_bootstrap",
			CorrelationID: "webui-local", TraceID: "webui-local"}
		app, createErr := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalAppID, AgentAppKey: "assistant", DisplayName: "WebUI Assistant"}, ChangeMetadata: appMetadata})
		if createErr != nil {
			return webUILocalBootstrap{}, createErr
		}
		draft, draftErr := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalAppID, ExpectedAppVersion: app.Version,
			Revision: agentapp.Revision{AgentKind: agentapp.AgentKindLLM, Instruction: webUILocalInstruction,
				ModelProfileID: webUILocalModelID, ModelProfileVersion: 1,
				ToolRefs: []agentapp.VersionedRef{{ID: localnote.ID, Version: localnote.Version, Required: true}}}, ChangeMetadata: appMetadata})
		if draftErr != nil {
			return webUILocalBootstrap{}, draftErr
		}
		if _, publishErr := apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
			Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
			ChangeMetadata: appMetadata}); publishErr != nil {
			return webUILocalBootstrap{}, publishErr
		}
		published, publishErr := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID,
			ExpectedTenantVersion: root.Version, Metadata: metadata,
			Payload: configdomain.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: webUILocalAppID, PolicyVersion: 1,
				ChannelBindings: []configdomain.ChannelBinding{{BindingID: webUILocalBindingID, Channel: "webui",
					ExternalAccountID: webUILocalAccountID, AgentAppID: webUILocalAppID,
					SecretRef: secrets.SecretRef{Ref: "secret://local/webui-verify", Version: 1}}}}})
		if publishErr != nil {
			return webUILocalBootstrap{}, publishErr
		}
		root = published.Tenant
	} else if err != nil {
		return webUILocalBootstrap{}, err
	}
	snapshot, err := configs.GetCurrent(ctx, webUILocalTenantID)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	root, snapshot, err = ensureWebUILocalToolControlPlane(ctx, tenants, apps, configs, governanceStore, root, snapshot)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	var binding configdomain.ChannelBinding
	for _, candidate := range snapshot.Payload.ChannelBindings {
		if candidate.BindingID == webUILocalBindingID && candidate.Channel == "webui" {
			binding = candidate
			break
		}
	}
	if binding.BindingID == "" || binding.ExternalAccountID != webUILocalAccountID {
		return webUILocalBootstrap{}, errors.New("existing local control plane is incompatible; recreate the Compose volume")
	}
	secretValues := []struct {
		scope secrets.Scope
		ref   secrets.SecretRef
		value []byte
	}{
		{payloadkey.Scope(webUILocalTenantID, 1), secrets.SecretRef{Ref: payloadKeyRef, Version: 1}, deriveLocalSecret("payload", configValue.Token)},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalBindingID, Purpose: secrets.PurposeChannelVerify,
			ResourceID: webUILocalBindingID, ResourceVersion: snapshot.ConfigVersion}, binding.SecretRef,
			[]byte(fmt.Sprintf(`{"token":%q,"external_account_id":%q}`, configValue.Token, webUILocalAccountID))},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalTenantID, Purpose: secrets.PurposeTenantIdentity,
			ResourceID: webUILocalTenantID, ResourceVersion: 1}, secrets.SecretRef{Ref: "secret://local/identity", Version: 1}, deriveLocalSecret("identity", configValue.Token)},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalTenantID, Purpose: secrets.PurposeTenantSession,
			ResourceID: webUILocalTenantID, ResourceVersion: 1}, secrets.SecretRef{Ref: "secret://local/session", Version: 1}, deriveLocalSecret("session", configValue.Token)},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: "worker-model", Purpose: secrets.PurposeModelCall,
			ResourceID: webUILocalModelID, ResourceVersion: 1}, secrets.SecretRef{Ref: "secret://local/deepseek", Version: 1}, apiKey},
	}
	for _, item := range secretValues {
		if err := writeLocalSecret(configValue.SecretRoot, item.scope, item.ref, item.value); err != nil {
			return webUILocalBootstrap{}, err
		}
	}
	secretStore, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	payloadResolver, err := payloadkey.New(secretStore, payloadKeyRef)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	route := ingress.BindingRoute{OpaqueBindingID: "webui-local-binding-v1", Channel: "webui",
		RouteKeyDigest: webui.RouteKeyDigest(configValue.RouteKey), TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
		ChannelBindingID: webUILocalBindingID, ExternalAccountID: webUILocalAccountID, TenantVersion: root.Version,
		BindingVersion: snapshot.ConfigVersion, SecretRef: binding.SecretRef,
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://local/identity", Version: 1},
		SessionSecretRef:  secrets.SecretRef{Ref: "secret://local/session", Version: 1}, Enabled: true}
	if err := ingresspostgres.New(db).PutBindingRoute(ctx, route); err != nil {
		return webUILocalBootstrap{}, err
	}
	return webUILocalBootstrap{Tenant: root, Config: snapshot, Route: route, SecretRoot: configValue.SecretRoot,
		PayloadKey: payloadResolver, SecretStore: secretStore, ProviderRepo: providers}, nil
}

type webUILocalPolicyStore interface {
	GetPolicy(context.Context, string, int64) (governance.PolicySnapshot, error)
	PublishPolicy(context.Context, governance.PolicySnapshot) error
}

func ensureWebUILocalToolControlPlane(ctx context.Context, tenants tenant.Repository, apps agentapp.Repository,
	configs configdomain.Repository, policies webUILocalPolicyStore, root tenant.Tenant, snapshot configdomain.Snapshot,
) (tenant.Tenant, configdomain.Snapshot, error) {
	if ctx == nil || tenants == nil || apps == nil || configs == nil || policies == nil || root.TenantID != webUILocalTenantID ||
		snapshot.TenantID != webUILocalTenantID || snapshot.Payload.PolicyVersion < 1 {
		return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("invalid WebUI local control plane")
	}
	app, err := apps.Get(ctx, webUILocalTenantID, webUILocalAppID)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	revision, err := apps.GetRevision(ctx, webUILocalTenantID, webUILocalAppID, app.CurrentRevision)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	policy, err := policies.GetPolicy(ctx, webUILocalTenantID, snapshot.Payload.PolicyVersion)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	appMetadata := agentapp.ChangeMetadata{ActorType: "system", ActorID: "webui-local", Reason: "local_graph_upgrade",
		CorrelationID: "webui-local-graph", TraceID: "webui-local-graph"}
	childRevision, err := ensureWebUILocalGraphChild(ctx, apps, appMetadata)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	if !webUILocalGraphRevisionReady(revision, childRevision) {
		revision = agentapp.Revision{AgentKind: agentapp.AgentKindGraph, AgentSpec: agentapp.AgentSpecV1{
			Nodes: []agentapp.AgentNodeSpecV1{{Key: "assistant", FailurePolicy: agentapp.FailurePolicyFailFast,
				AgentRef: agentapp.PublishedAgentRef{AgentAppID: webUILocalChildAppID, Revision: childRevision.Revision,
					ContentDigest: childRevision.ContentDigest}}},
			EntryNode: "assistant", MaxConcurrency: 1,
			Checkpoint: agentapp.CheckpointPolicyV1{Required: true, Namespace: "webui-local"},
		}}
		draft, createErr := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalAppID, ExpectedAppVersion: app.Version, Revision: revision, ChangeMetadata: appMetadata})
		if createErr != nil {
			return tenant.Tenant{}, configdomain.Snapshot{}, createErr
		}
		if _, publishErr := apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
			Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
			ChangeMetadata: appMetadata}); publishErr != nil {
			return tenant.Tenant{}, configdomain.Snapshot{}, publishErr
		}
	}
	if webUILocalPolicyReady(policy.Policy) {
		return root, snapshot, nil
	}
	if policy.Version == int64(^uint64(0)>>1) {
		return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("WebUI local policy version exhausted")
	}
	policy.Policy.Tools = upsertWebUILocalToolRule(policy.Policy.Tools)
	policy.Version++
	policy.PublishedAt = time.Now().UTC()
	policy.ContentDigest, _, err = governance.PolicyDigest(policy.Policy)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	if err = policies.PublishPolicy(ctx, policy); err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	payload := snapshot.Payload
	payload.PolicyVersion = policy.Version
	metadata := tenant.ChangeMetadata{ActorType: "system", ActorID: "webui-local", ReasonCode: "local_tool_upgrade",
		CorrelationID: "webui-local-tool", TraceID: "webui-local-tool"}
	published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID,
		ExpectedTenantVersion: root.Version, Payload: payload, Metadata: metadata})
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	return published.Tenant, published.Snapshot, nil
}

func ensureWebUILocalGraphChild(ctx context.Context, apps agentapp.Repository, metadata agentapp.ChangeMetadata) (agentapp.Revision, error) {
	app, err := apps.Get(ctx, webUILocalTenantID, webUILocalChildAppID)
	if errors.Is(err, agentapp.ErrNotFound) {
		app, err = apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalChildAppID, AgentAppKey: "assistant-llm", DisplayName: "WebUI Assistant LLM"}, ChangeMetadata: metadata})
	}
	if err != nil {
		return agentapp.Revision{}, err
	}
	if app.CurrentRevision > 0 {
		current, currentErr := apps.GetRevision(ctx, webUILocalTenantID, webUILocalChildAppID, app.CurrentRevision)
		if currentErr != nil {
			return agentapp.Revision{}, currentErr
		}
		if webUILocalLLMRevisionReady(current) {
			return current, nil
		}
	}
	draft, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID,
		AgentAppID: webUILocalChildAppID, ExpectedAppVersion: app.Version,
		Revision: agentapp.Revision{AgentKind: agentapp.AgentKindLLM, Instruction: webUILocalInstruction,
			ModelProfileID: webUILocalModelID, ModelProfileVersion: 1,
			ToolRefs: []agentapp.VersionedRef{{ID: localnote.ID, Version: localnote.Version, Required: true}}}, ChangeMetadata: metadata})
	if err != nil {
		return agentapp.Revision{}, err
	}
	published, err := apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalChildAppID,
		Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
		ChangeMetadata: metadata})
	if err != nil {
		return agentapp.Revision{}, err
	}
	return published.Revision, nil
}

func webUILocalLLMRevisionReady(value agentapp.Revision) bool {
	if value.AgentKind != agentapp.AgentKindLLM || value.Instruction != webUILocalInstruction ||
		value.ModelProfileID != webUILocalModelID || value.ModelProfileVersion != 1 {
		return false
	}
	for _, ref := range value.ToolRefs {
		if ref.ID == localnote.ID {
			return ref.Version == localnote.Version && ref.Required
		}
	}
	return false
}

func webUILocalGraphRevisionReady(value, child agentapp.Revision) bool {
	if value.AgentKind != agentapp.AgentKindGraph || len(value.AgentSpec.Nodes) != 1 ||
		value.AgentSpec.EntryNode != "assistant" || value.AgentSpec.MaxConcurrency != 1 ||
		!value.AgentSpec.Checkpoint.Required || value.AgentSpec.Checkpoint.Namespace != "webui-local" {
		return false
	}
	node := value.AgentSpec.Nodes[0]
	return node.Key == "assistant" && node.FailurePolicy == agentapp.FailurePolicyFailFast &&
		node.AgentRef.AgentAppID == webUILocalChildAppID && node.AgentRef.Revision == child.Revision &&
		node.AgentRef.ContentDigest == child.ContentDigest
}

func webUILocalPolicyReady(value governance.PolicyV1) bool {
	for _, rule := range value.Tools {
		if rule.ToolID == localnote.ID {
			return rule.Version == localnote.Version && rule.Dangerous && rule.ConfirmationSupported
		}
	}
	return false
}

func upsertWebUILocalToolRule(values []governance.ToolRule) []governance.ToolRule {
	result := append([]governance.ToolRule(nil), values...)
	for index := range result {
		if result[index].ToolID == localnote.ID {
			result[index] = governance.ToolRule{ToolID: localnote.ID, Version: localnote.Version, Dangerous: true, ConfirmationSupported: true}
			return result
		}
	}
	return append(result, governance.ToolRule{ToolID: localnote.ID, Version: localnote.Version, Dangerous: true, ConfirmationSupported: true})
}

func deriveLocalSecret(kind, token string) []byte {
	value := sha256.Sum256([]byte("trpc-webui-local-v1\x00" + kind + "\x00" + token))
	return append([]byte(nil), value[:]...)
}

func writeLocalSecret(root string, scope secrets.Scope, ref secrets.SecretRef, value []byte) error {
	name, err := secretfs.StableFilename(scope, ref)
	if err != nil {
		return err
	}
	path := filepath.Join(root, name)
	if existing, readErr := os.ReadFile(path); readErr == nil {
		if !bytes.Equal(existing, value) {
			return errors.New("local secret generation changed; recreate the WebUI container")
		}
		return nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	return os.WriteFile(path, value, 0o600)
}
