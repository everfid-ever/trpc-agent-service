package qdrant

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
)

func TestAdapterQdrantIntegration(t *testing.T) {
	endpoint := os.Getenv("TRPC_QDRANT_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("TRPC_QDRANT_TEST_ENDPOINT is required")
	}
	collection := "trpc_knowledge_adapter_contract"
	// Qdrant's Cosine collection normalizes stored vectors. The migration image
	// digest intentionally commits the original embedding bytes, so the
	// collection must use a non-normalizing distance such as Dot.
	request, err := http.NewRequest(http.MethodPut, endpoint+"/collections/"+collection, bytes.NewBufferString(`{"vectors":{"size":2,"distance":"Dot"}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("create collection status=%d", response.StatusCode)
	}
	t.Cleanup(func() {
		request, _ := http.NewRequest(http.MethodDelete, endpoint+"/collections/"+collection, nil)
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			response.Body.Close()
		}
	})
	adapter, err := New(Config{Endpoint: endpoint, Collection: collection, VectorSize: 2, SnapshotWatermark: "integration-snapshot", AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, err := knowledgedriver.ImageDigest(image)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "integration", MutationID: "integration-mutation", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	loaded, err := adapter.LoadChunk(context.Background(), image.Key)
	if err != nil || loaded.Key != image.Key {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	page, err := adapter.PageChunks(context.Background(), knowledgedriver.PageRequest{TenantID: image.Key.TenantID, SnapshotWatermark: "integration-snapshot", Limit: 10})
	if err != nil || len(page.Chunks) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}
