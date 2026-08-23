package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	appmemory "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configmemory "github.com/liuzengh/trpc-agent-service/trpcservice/config/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
)

type headerPrincipalResolver struct{}

func (headerPrincipalResolver) Resolve(r *http.Request) (Principal, error) {
	return Principal{Authenticated: r.Header.Get("X-Authenticated") == "true", TenantID: r.Header.Get("X-Admin-Tenant"), SubjectID: "operator", CanManage: true}, nil
}

func adminSetup(t *testing.T) (*Handler, *tenantmemory.Repository) {
	t.Helper()
	ctx := context.Background()
	meta := tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: "setup", CorrelationID: "correlation", TraceID: "trace"}
	tenants := tenantmemory.New()
	if _, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "A"}, ChangeMetadata: meta}); err != nil {
		t.Fatal(err)
	}
	apps := appmemory.New()
	appMeta := agentapp.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "setup", CorrelationID: "correlation", TraceID: "trace"}
	app, err := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app", AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: "tenant-a", AgentAppID: "app", ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: "llm", Instruction: "help", ModelProfileID: "mock", ModelProfileVersion: 1}, ChangeMetadata: appMeta})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apps.Publish(ctx, agentapp.PublishInput{TenantID: "tenant-a", AgentAppID: "app", Revision: rev.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: appMeta}); err != nil {
		t.Fatal(err)
	}
	handler := &Handler{Service: Service{Configs: configmemory.New(tenants, apps)}, Principals: headerPrincipalResolver{}}
	return handler, tenants
}

func TestAdminHTTPValidatePublishAndTenantScope(t *testing.T) {
	handler, tenants := adminSetup(t)
	payload := config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 1}
	body, _ := json.Marshal(payload)
	request := func(method, path, principalTenant string) *http.Request {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("X-Authenticated", "true")
		req.Header.Set("X-Admin-Tenant", principalTenant)
		req.Header.Set("X-Reason-Code", "test")
		req.Header.Set("X-Correlation-ID", "correlation")
		req.Header.Set("X-Trace-ID", "trace")
		return req
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/tenant-a/configs/validate", "tenant-a"))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("validate=%d %s", rec.Code, rec.Body.String())
	}
	current, err := tenants.Get(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 1 || current.ActiveConfigVersion != 0 {
		t.Fatalf("validate had side effects: %#v", current)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "tenant-b"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("scope=%d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodPost, "/v1/tenants/tenant-a/configs/publish?expected_version=1", "tenant-a"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("publish=%d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, request(http.MethodGet, "/v1/tenants/tenant-a/configs", "tenant-a"))
	if rec.Code != http.StatusOK {
		t.Fatalf("get=%d %s", rec.Code, rec.Body.String())
	}
}
