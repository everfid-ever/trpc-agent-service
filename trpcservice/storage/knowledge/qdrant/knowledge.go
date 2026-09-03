package qdrant

import (
	"context"
	"fmt"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

// Knowledge exposes one immutable tenant Knowledge version through the public
// tRPC-Agent-Go Knowledge interface. TenantKnowledge remains the outer guard;
// this binding repeats its scope validation so it is also safe when composed
// directly by a runtime bundle.
type Knowledge struct {
	Adapter          *Adapter
	TenantID         string
	KnowledgeID      string
	KnowledgeVersion int64
}

func (k Knowledge) Search(ctx context.Context, request *upstream.SearchRequest) (*upstream.SearchResult, error) {
	if k.Adapter == nil || k.TenantID == "" || k.KnowledgeID == "" || k.KnowledgeVersion < 1 || request == nil || request.Query == "" {
		return nil, runtime.ErrInvariantViolation
	}
	if err := k.validateFilter(request.SearchFilter); err != nil {
		return nil, err
	}
	limit := request.MaxResults
	if limit <= 0 {
		limit = 10
	}
	if limit > 64 || math.IsNaN(request.MinScore) || math.IsInf(request.MinScore, 0) {
		return nil, runtime.ErrInvariantViolation
	}
	in := knowledgedriver.SearchRequest{TenantID: k.TenantID, KnowledgeID: k.KnowledgeID, KnowledgeVersion: k.KnowledgeVersion, Query: request.Query}
	points, err := k.Adapter.search(ctx, in, limit, request.MinScore)
	if err != nil {
		return nil, err
	}
	result := &upstream.SearchResult{Documents: make([]*upstream.Result, 0, len(points))}
	seen := make(map[knowledgedriver.ChunkKey]struct{}, len(points))
	for _, point := range points {
		image, err := k.Adapter.checkedSearchImage(in, point)
		if err != nil {
			return nil, err
		}
		if image.Operation == knowledgedriver.OperationDelete {
			continue
		}
		if _, exists := seen[image.Key]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		seen[image.Key] = struct{}{}
		doc := documentFor(image)
		item := &upstream.Result{Document: doc, Score: point.Score}
		result.Documents = append(result.Documents, item)
		if result.Document == nil {
			result.Document, result.Score, result.Text = doc, item.Score, doc.Content
		}
	}
	return result, nil
}

func (k Knowledge) validateFilter(filter *upstream.SearchFilter) error {
	if filter == nil {
		return nil
	}
	if len(filter.DocumentIDs) != 0 || filter.FilterCondition != nil {
		return runtime.ErrCapabilityUnsupported
	}
	want := map[string]any{"tenant_id": k.TenantID, "knowledge_id": k.KnowledgeID, "knowledge_version": k.KnowledgeVersion}
	for key, value := range filter.Metadata {
		expected, known := want[key]
		if !known || fmt.Sprint(value) != fmt.Sprint(expected) {
			return runtime.ErrTenantScope
		}
	}
	return nil
}

func documentFor(image knowledgedriver.ChunkImage) *document.Document {
	metadata := make(map[string]any, len(image.Metadata)+3)
	for key, value := range image.Metadata {
		metadata[key] = value
	}
	metadata["tenant_id"] = image.Key.TenantID
	metadata["knowledge_id"] = image.Key.KnowledgeID
	metadata["knowledge_version"] = image.Key.KnowledgeVersion
	name, _ := metadata["title"].(string)
	return &document.Document{ID: image.Key.ChunkID, Name: name, Content: image.Content, Metadata: metadata}
}

var _ upstream.Knowledge = Knowledge{}
