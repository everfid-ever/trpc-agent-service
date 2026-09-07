package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type demoModelStub struct{}

func (demoModelStub) ResolveModel(context.Context, string, profile.VersionedRef) (model.Model, error) {
	return demoResponseModel{}, nil
}

type demoResponseModel struct{}

func (demoResponseModel) GenerateContent(_ context.Context, _ *model.Request) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{ID: "demo-response", Model: "fake-deterministic-v1", Done: true,
		Choices: []model.Choice{{Message: model.NewAssistantMessage("fixed response")}}}
	close(responses)
	return responses, nil
}

func (demoResponseModel) Info() model.Info { return model.Info{Name: "fake-deterministic-v1"} }

func TestDemoHTTPHandlerServesHealthReadinessAndFakeChat(t *testing.T) {
	handler := newDemoHTTPHandler(demoModelStub{}, func(context.Context) error { return nil })
	for _, path := range []string{"/healthz", "/readyz"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s status=%d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"message":"same input"}`)))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"response":"fixed response"`) {
		t.Fatalf("chat status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDemoHTTPHandlerRejectsInvalidChat(t *testing.T) {
	handler := newDemoHTTPHandler(demoModelStub{}, func(context.Context) error { return nil })
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(`{"unexpected":true}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", response.Code)
	}
}
