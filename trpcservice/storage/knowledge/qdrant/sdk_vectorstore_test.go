package qdrant

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestSDKReadOnlyVectorStoreDelegatesSearchAndRechecksEnvelope(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	sdkDocument := sdkMirrorDocument(image, digest, "snapshot-a")
	var gotConfig sdkStoreConfig
	var gotQuery *vectorstore.SearchQuery
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration}, func(_ context.Context, config sdkStoreConfig) (vectorstore.VectorStore, error) {
		gotConfig = config
		return &fakeSDKVectorStore{search: func(query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			gotQuery = query
			return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: pointID(image.Key)}, Score: 0.9}}}, nil
		}, get: func(string) (*document.Document, []float64, error) { return sdkDocument, []float64{0.25, 0.75}, nil }}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{Query: "query", Vector: []float64{0.25, 0.75}, Limit: 2,
		SearchMode: vectorstore.SearchModeVector, Filter: &vectorstore.SearchFilter{Metadata: map[string]any{
			"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion, "title": "document",
		}}})
	if err != nil || len(result.Results) != 1 || result.Results[0].Document.ID != image.Key.ChunkID {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if gotConfig.host != "127.0.0.1" || gotConfig.port != 6334 || gotConfig.tls || gotConfig.collection != "knowledge" || gotConfig.dimension != 2 {
		t.Fatalf("sdk config=%+v", gotConfig)
	}
	if gotQuery == nil || gotQuery.Filter.Metadata[sdkScopeMetadataKey+".tenant_id"] != image.Key.TenantID || gotQuery.Filter.Metadata["title"] != "document" {
		t.Fatalf("sdk query=%+v", gotQuery)
	}
}

func TestSDKReadOnlyVectorStoreFailsClosedForForeignSDKCandidate(t *testing.T) {
	backend := newFakeQdrant()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	adapter, err := New(Config{Endpoint: server.URL, Collection: "knowledge", VectorSize: 2, SnapshotWatermark: "snapshot-a",
		VectorGeneration: "generation", RuntimeEngine: "sdk", GRPCPort: 6334, AllowInsecureHTTP: true}, fixedEmbedder{})
	if err != nil {
		t.Fatal(err)
	}
	image := fixtureImage()
	digest, _ := knowledgedriver.ImageDigest(image)
	if _, err := adapter.ApplyChunk(context.Background(), knowledgedriver.ApplyRequest{TenantID: image.Key.TenantID, MigrationID: "migration-a", MutationID: "mutation-a", Epoch: 1, Image: image, ImageDigest: digest}); err != nil {
		t.Fatal(err)
	}
	store, err := newSDKReadOnlyVectorStore(adapter, RuntimeScope{TenantID: image.Key.TenantID, KnowledgeID: image.Key.KnowledgeID,
		KnowledgeVersion: image.Key.KnowledgeVersion, EmbedderProfile: image.EmbeddingProfileID, EmbedderVersion: image.EmbeddingVersion,
		VectorGeneration: image.VectorGeneration}, func(context.Context, sdkStoreConfig) (vectorstore.VectorStore, error) {
		return &fakeSDKVectorStore{search: func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
			return &vectorstore.SearchResult{Results: []*vectorstore.ScoredDocument{{Document: &document.Document{ID: "foreign-point"}, Score: 0.9}}}, nil
		}, get: func(string) (*document.Document, []float64, error) {
			return &document.Document{ID: "foreign-point", Metadata: map[string]any{}}, []float64{0.25, 0.75}, nil
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{Query: "query", Vector: []float64{0.25, 0.75}, SearchMode: vectorstore.SearchModeVector,
		Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion}}})
	if !errors.Is(err, runtime.ErrInvariantViolation) || result != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func sdkMirrorDocument(image knowledgedriver.ChunkImage, digest, watermark string) *document.Document {
	return &document.Document{ID: pointID(image.Key), Content: image.Content, Metadata: map[string]any{
		"title": image.Metadata["title"], sdkScopeMetadataKey: map[string]any{
			"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion, "chunk_id": image.Key.ChunkID,
			"revision": image.Revision, "operation": string(image.Operation), "source_digest": image.SourceDigest, "content_digest": image.ContentDigest,
			"metadata_digest": image.MetadataDigest, "embedding_profile_id": image.EmbeddingProfileID, "embedding_version": image.EmbeddingVersion,
			"vector_generation": image.VectorGeneration, "image_digest": digest, "snapshot_watermark": watermark,
		},
	}}
}

type fakeSDKVectorStore struct {
	search func(*vectorstore.SearchQuery) (*vectorstore.SearchResult, error)
	get    func(string) (*document.Document, []float64, error)
}

func (s *fakeSDKVectorStore) Add(context.Context, *document.Document, []float64) error {
	return errors.New("unexpected add")
}
func (s *fakeSDKVectorStore) Update(context.Context, *document.Document, []float64) error {
	return errors.New("unexpected update")
}
func (s *fakeSDKVectorStore) Delete(context.Context, string) error {
	return errors.New("unexpected delete")
}
func (s *fakeSDKVectorStore) DeleteByFilter(context.Context, ...vectorstore.DeleteOption) error {
	return errors.New("unexpected delete by filter")
}
func (s *fakeSDKVectorStore) UpdateByFilter(context.Context, ...vectorstore.UpdateByFilterOption) (int64, error) {
	return 0, errors.New("unexpected update by filter")
}
func (s *fakeSDKVectorStore) Count(context.Context, ...vectorstore.CountOption) (int, error) {
	return 0, errors.New("unexpected count")
}
func (s *fakeSDKVectorStore) GetMetadata(context.Context, ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return nil, errors.New("unexpected metadata")
}
func (s *fakeSDKVectorStore) Get(_ context.Context, id string) (*document.Document, []float64, error) {
	if s.get == nil {
		return nil, nil, errors.New("unexpected get")
	}
	return s.get(id)
}
func (s *fakeSDKVectorStore) Search(_ context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	return s.search(query)
}
func (*fakeSDKVectorStore) Close() error { return nil }

var _ vectorstore.VectorStore = (*fakeSDKVectorStore)(nil)
