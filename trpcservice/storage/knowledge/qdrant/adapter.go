// Package qdrant adapts the public Qdrant HTTP API to the Knowledge migration
// ports. It is deliberately tenant-filtered at every read, search and scroll.
package qdrant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// TokenSource resolves a Qdrant API key at request time. Production callers
// should implement it with a tenant-scoped secret provider; Adapter never
// retains the secret after the request finishes.
type TokenSource interface {
	Token(context.Context) (string, error)
}

type Config struct {
	Endpoint   string
	Collection string
	// Collection must preserve embedding bytes (for example Qdrant Dot
	// distance). Cosine normalization changes the image digest and is rejected
	// by read-back verification.
	VectorSize        int
	SnapshotWatermark string
	AllowInsecureHTTP bool
	HTTPClient        *http.Client
	TokenSource       TokenSource
}

// Embedder supplies query vectors for migration sample probes. It is kept
// separate from Qdrant because embeddings are a fixed Knowledge version fact.
type Embedder interface {
	Embed(context.Context, string) ([]float32, error)
}

type Adapter struct {
	endpoint          *url.URL
	collection        string
	vectorSize        int
	snapshotWatermark string
	client            *http.Client
	tokens            TokenSource
	embedder          Embedder
}

func New(config Config, embedder Embedder) (*Adapter, error) {
	endpoint, err := url.Parse(strings.TrimSpace(config.Endpoint))
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		(endpoint.Scheme != "https" && !(config.AllowInsecureHTTP && endpoint.Scheme == "http")) ||
		!validName(config.Collection) || config.VectorSize < 1 || strings.TrimSpace(config.SnapshotWatermark) == "" {
		return nil, runtime.ErrInvariantViolation
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 20 * time.Second}
	}
	return &Adapter{endpoint: endpoint, collection: config.Collection, vectorSize: config.VectorSize,
		snapshotWatermark: config.SnapshotWatermark, client: config.HTTPClient, tokens: config.TokenSource, embedder: embedder}, nil
}

func (a *Adapter) LoadChunk(ctx context.Context, key knowledgedriver.ChunkKey) (knowledgedriver.ChunkImage, error) {
	if !validKey(key) {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	point, err := a.point(ctx, pointID(key))
	if err != nil {
		return knowledgedriver.ChunkImage{}, err
	}
	image, err := a.imageFromPoint(point)
	if err != nil {
		return knowledgedriver.ChunkImage{}, err
	}
	if image.Key != key {
		return knowledgedriver.ChunkImage{}, runtime.ErrTenantScope
	}
	return image, nil
}

func (a *Adapter) ApplyChunk(ctx context.Context, in knowledgedriver.ApplyRequest) (knowledgedriver.ApplyResult, error) {
	if a == nil || !validKey(in.Image.Key) || in.TenantID != in.Image.Key.TenantID || in.MigrationID == "" || in.MutationID == "" || in.Epoch < 1 {
		return knowledgedriver.ApplyResult{}, runtime.ErrInvariantViolation
	}
	digest, err := knowledgedriver.ImageDigest(in.Image)
	if err != nil || digest != in.ImageDigest {
		return knowledgedriver.ApplyResult{}, runtime.ErrInvariantViolation
	}
	id := pointID(in.Image.Key)
	if existing, err := a.point(ctx, id); err == nil {
		old, decodeErr := a.imageFromPoint(existing)
		if decodeErr != nil {
			return knowledgedriver.ApplyResult{}, decodeErr
		}
		oldDigest, digestErr := knowledgedriver.ImageDigest(old)
		if digestErr != nil {
			return knowledgedriver.ApplyResult{}, digestErr
		}
		oldMutation, _ := text(existing.Payload, "mutation_id")
		if oldMutation == in.MutationID {
			if oldDigest != digest || old.Key != in.Image.Key {
				return knowledgedriver.ApplyResult{}, runtime.ErrIdempotencyCollision
			}
			return knowledgedriver.ApplyResult{Revision: old.Revision, Digest: oldDigest}, nil
		}
		if old.Revision > in.Image.Revision || (old.Revision == in.Image.Revision && oldDigest != digest) {
			return knowledgedriver.ApplyResult{}, runtime.ErrIdempotencyCollision
		}
	} else if !errors.Is(err, runtime.ErrNotFound) {
		return knowledgedriver.ApplyResult{}, err
	}
	payload := encodeImage(in.Image)
	payload["mutation_id"] = in.MutationID
	payload["migration_id"] = in.MigrationID
	payload["migration_epoch"] = in.Epoch
	payload["image_digest"] = digest
	payload["snapshot_watermark"] = a.snapshotWatermark
	vector := in.Image.Vector
	if in.Image.Operation == knowledgedriver.OperationDelete {
		vector = make([]float32, a.vectorSize)
	}
	if len(vector) != a.vectorSize {
		return knowledgedriver.ApplyResult{}, runtime.ErrInvariantViolation
	}
	if err := a.request(ctx, http.MethodPut, "/collections/"+url.PathEscape(a.collection)+"/points?wait=true", map[string]any{
		"points": []any{map[string]any{"id": id, "vector": vector, "payload": payload}},
	}, nil); err != nil {
		return knowledgedriver.ApplyResult{}, err
	}
	return knowledgedriver.ApplyResult{Revision: in.Image.Revision, Digest: digest}, nil
}

func (a *Adapter) PageChunks(ctx context.Context, in knowledgedriver.PageRequest) (knowledgedriver.Page, error) {
	if a == nil || strings.TrimSpace(in.TenantID) == "" || in.Limit < 1 || in.SnapshotWatermark != a.snapshotWatermark {
		return knowledgedriver.Page{}, runtime.ErrInvariantViolation
	}
	points, err := a.scroll(ctx, in.TenantID)
	if err != nil {
		return knowledgedriver.Page{}, err
	}
	images := make([]knowledgedriver.ChunkImage, 0, len(points))
	for _, point := range points {
		image, err := a.imageFromPoint(point)
		if err != nil {
			return knowledgedriver.Page{}, err
		}
		if image.Key.TenantID != in.TenantID {
			return knowledgedriver.Page{}, runtime.ErrTenantScope
		}
		images = append(images, image)
	}
	sort.Slice(images, func(i, j int) bool { return checkpoint(images[i].Key) < checkpoint(images[j].Key) })
	start := 0
	for start < len(images) && checkpoint(images[start].Key) <= in.After {
		start++
	}
	end := start + in.Limit
	if end > len(images) {
		end = len(images)
	}
	page := append([]knowledgedriver.ChunkImage(nil), images[start:end]...)
	if len(page) == 0 {
		return knowledgedriver.Page{NextCheckpoint: "empty:" + a.snapshotWatermark, Complete: true}, nil
	}
	return knowledgedriver.Page{Chunks: page, NextCheckpoint: checkpoint(page[len(page)-1].Key), Complete: end == len(images)}, nil
}

func (a *Adapter) Fingerprint(ctx context.Context, tenantID, watermark string) (knowledgedriver.Fingerprint, error) {
	if a == nil || strings.TrimSpace(tenantID) == "" || (watermark != "" && watermark != a.snapshotWatermark) {
		return knowledgedriver.Fingerprint{}, runtime.ErrInvariantViolation
	}
	points, err := a.scroll(ctx, tenantID)
	if err != nil {
		return knowledgedriver.Fingerprint{}, err
	}
	digests := make([]string, 0, len(points))
	for _, point := range points {
		image, err := a.imageFromPoint(point)
		if err != nil {
			return knowledgedriver.Fingerprint{}, err
		}
		if image.Key.TenantID != tenantID {
			return knowledgedriver.Fingerprint{}, runtime.ErrTenantScope
		}
		digest, err := knowledgedriver.ImageDigest(image)
		if err != nil {
			return knowledgedriver.Fingerprint{}, err
		}
		digests = append(digests, checkpoint(image.Key)+":"+strconv.FormatInt(image.Revision, 10)+":"+digest)
	}
	sort.Strings(digests)
	return knowledgedriver.Fingerprint{Count: int64(len(digests)), Digest: hash(digests...), Watermark: a.snapshotWatermark}, nil
}

func (a *Adapter) Search(ctx context.Context, in knowledgedriver.SearchRequest) ([]knowledgedriver.ChunkKey, error) {
	results, err := a.search(ctx, in, 64, 0)
	if err != nil {
		return nil, err
	}
	keys := make([]knowledgedriver.ChunkKey, 0, len(results))
	seen := make(map[knowledgedriver.ChunkKey]struct{}, len(results))
	for _, result := range results {
		image, err := a.checkedSearchImage(in, result)
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
		keys = append(keys, image.Key)
	}
	return keys, nil
}

func (a *Adapter) search(ctx context.Context, in knowledgedriver.SearchRequest, limit int, minScore float64) ([]point, error) {
	if a == nil || a.embedder == nil || strings.TrimSpace(in.TenantID) == "" || strings.TrimSpace(in.KnowledgeID) == "" || in.KnowledgeVersion < 1 || in.Query == "" {
		return nil, runtime.ErrInvariantViolation
	}
	if limit < 1 || limit > 64 || math.IsNaN(minScore) || math.IsInf(minScore, 0) {
		return nil, runtime.ErrInvariantViolation
	}
	vector, err := a.embedder.Embed(ctx, in.Query)
	if err != nil || len(vector) != a.vectorSize {
		return nil, runtime.ErrBackendUnavailable
	}
	var response struct {
		Result []point `json:"result"`
	}
	if err := a.request(ctx, http.MethodPost, "/collections/"+url.PathEscape(a.collection)+"/points/search", map[string]any{
		"vector": vector, "limit": limit, "score_threshold": minScore, "with_payload": true, "with_vector": true, "filter": tenantFilter(in.TenantID),
	}, &response); err != nil {
		return nil, err
	}
	return response.Result, nil
}

func (a *Adapter) checkedSearchImage(in knowledgedriver.SearchRequest, result point) (knowledgedriver.ChunkImage, error) {
	// Check the returned tenant before the integrity envelope. This keeps a
	// backend that ignored the mandatory Qdrant filter classified as a
	// tenant-scope failure even when its point ID/digest no longer matches
	// the malicious payload.
	resultTenant, ok := text(result.Payload, "tenant_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	if resultTenant != in.TenantID {
		return knowledgedriver.ChunkImage{}, runtime.ErrTenantScope
	}
	image, err := a.imageFromPoint(result)
	if err != nil {
		return knowledgedriver.ChunkImage{}, err
	}
	if image.Key.TenantID != in.TenantID || image.Key.KnowledgeID != in.KnowledgeID || image.Key.KnowledgeVersion != in.KnowledgeVersion {
		return knowledgedriver.ChunkImage{}, runtime.ErrTenantScope
	}
	return image, nil
}

type point struct {
	ID      any            `json:"id"`
	Payload map[string]any `json:"payload"`
	Vector  []float32      `json:"vector"`
	Score   float64        `json:"score"`
}

func (a *Adapter) point(ctx context.Context, id string) (point, error) {
	var response struct {
		Result point `json:"result"`
	}
	err := a.request(ctx, http.MethodGet, "/collections/"+url.PathEscape(a.collection)+"/points/"+url.PathEscape(id)+"?with_payload=true&with_vector=true", nil, &response)
	return response.Result, err
}

// imageFromPoint verifies the adapter-owned envelope before returning the
// backend-neutral image. A matching filter alone is not a tenant boundary:
// Qdrant responses are checked again because a malformed proxy or collection
// configuration must fail closed before a cutover can use the result.
func (a *Adapter) imageFromPoint(value point) (knowledgedriver.ChunkImage, error) {
	watermark, ok := text(value.Payload, "snapshot_watermark")
	if !ok || watermark != a.snapshotWatermark {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	image, err := decodeImage(value.Payload, value.Vector)
	if err != nil {
		return knowledgedriver.ChunkImage{}, err
	}
	if id, ok := value.ID.(string); !ok || id != pointID(image.Key) {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	want, ok := text(value.Payload, "image_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	got, err := knowledgedriver.ImageDigest(image)
	if err != nil || got != want {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	return image, nil
}

func (a *Adapter) scroll(ctx context.Context, tenantID string) ([]point, error) {
	offset := any(nil)
	points := make([]point, 0)
	for {
		var response struct {
			Result struct {
				Points []point `json:"points"`
				Next   any     `json:"next_page_offset"`
			} `json:"result"`
		}
		body := map[string]any{"limit": 256, "with_payload": true, "with_vector": true, "filter": tenantFilter(tenantID)}
		if offset != nil {
			body["offset"] = offset
		}
		if err := a.request(ctx, http.MethodPost, "/collections/"+url.PathEscape(a.collection)+"/points/scroll", body, &response); err != nil {
			return nil, err
		}
		points = append(points, response.Result.Points...)
		if response.Result.Next == nil {
			return points, nil
		}
		offset = response.Result.Next
	}
}

func (a *Adapter) request(ctx context.Context, method, path string, body any, out any) error {
	if a == nil {
		return runtime.ErrBackendUnavailable
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return runtime.ErrInvariantViolation
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.endpoint.String()+path, reader)
	if err != nil {
		return runtime.ErrInvariantViolation
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.tokens != nil {
		token, err := a.tokens.Token(ctx)
		if err != nil || strings.TrimSpace(token) == "" {
			return runtime.ErrBackendUnavailable
		}
		req.Header.Set("api-key", token)
	}
	response, err := a.client.Do(req)
	if err != nil {
		return runtime.ErrBackendUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return runtime.ErrNotFound
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return runtime.ErrBackendUnavailable
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(out); err != nil {
		return runtime.ErrBackendUnavailable
	}
	return nil
}

func tenantFilter(tenantID string) map[string]any {
	return map[string]any{"must": []any{map[string]any{"key": "tenant_id", "match": map[string]any{"value": tenantID}}}}
}
func pointID(key knowledgedriver.ChunkKey) string {
	sum := sha256.Sum256([]byte(key.TenantID + "\x00" + key.KnowledgeID + "\x00" + strconv.FormatInt(key.KnowledgeVersion, 10) + "\x00" + key.ChunkID))
	raw := hex.EncodeToString(sum[:])
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:32]
}
func checkpoint(key knowledgedriver.ChunkKey) string {
	return hex.EncodeToString([]byte(key.KnowledgeID)) + ":" + fmt.Sprintf("%020d", key.KnowledgeVersion) + ":" + hex.EncodeToString([]byte(key.ChunkID))
}
func hash(values ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return hex.EncodeToString(sum[:])
}
func validName(value string) bool {
	if value == "" || len(value) > 255 {
		return false
	}
	for _, ch := range value {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return false
		}
	}
	return true
}
func validKey(key knowledgedriver.ChunkKey) bool {
	return key.TenantID != "" && key.KnowledgeID != "" && key.KnowledgeVersion >= 1 && key.ChunkID != ""
}

func encodeImage(image knowledgedriver.ChunkImage) map[string]any {
	return map[string]any{"tenant_id": image.Key.TenantID, "knowledge_id": image.Key.KnowledgeID, "knowledge_version": image.Key.KnowledgeVersion, "chunk_id": image.Key.ChunkID, "revision": image.Revision, "operation": string(image.Operation), "source_digest": image.SourceDigest, "content_digest": image.ContentDigest, "metadata_digest": image.MetadataDigest, "embedding_profile_id": image.EmbeddingProfileID, "embedding_version": image.EmbeddingVersion, "vector_generation": image.VectorGeneration, "content": image.Content, "metadata": image.Metadata}
}
func decodeImage(payload map[string]any, vector []float32) (knowledgedriver.ChunkImage, error) {
	tenant, ok := text(payload, "tenant_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	knowledge, ok := text(payload, "knowledge_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	chunk, ok := text(payload, "chunk_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	version, ok := int64Value(payload, "knowledge_version")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	revision, ok := int64Value(payload, "revision")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	operation, ok := text(payload, "operation")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	source, ok := text(payload, "source_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	contentDigest, ok := text(payload, "content_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	metadataDigest, ok := text(payload, "metadata_digest")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	profile, ok := text(payload, "embedding_profile_id")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	embeddingVersion, ok := int64Value(payload, "embedding_version")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	generation, ok := text(payload, "vector_generation")
	if !ok {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	content, _ := text(payload, "content")
	metadata, err := decodeMetadata(payload)
	if err != nil {
		return knowledgedriver.ChunkImage{}, err
	}
	image := knowledgedriver.ChunkImage{Key: knowledgedriver.ChunkKey{TenantID: tenant, KnowledgeID: knowledge, KnowledgeVersion: version, ChunkID: chunk}, Revision: revision, Operation: knowledgedriver.Operation(operation), SourceDigest: source, ContentDigest: contentDigest, MetadataDigest: metadataDigest, EmbeddingProfileID: profile, EmbeddingVersion: embeddingVersion, VectorGeneration: generation, Content: content, Metadata: metadata, Vector: vector}
	if image.Operation == knowledgedriver.OperationDelete {
		image.Content = ""
		image.Metadata = nil
		image.Vector = nil
	}
	if _, err := knowledgedriver.ImageDigest(image); err != nil {
		return knowledgedriver.ChunkImage{}, runtime.ErrInvariantViolation
	}
	return image, nil
}

func decodeMetadata(payload map[string]any) (map[string]string, error) {
	raw, exists := payload["metadata"]
	if !exists || raw == nil {
		return nil, nil
	}
	values, ok := raw.(map[string]any)
	if !ok {
		return nil, runtime.ErrInvariantViolation
	}
	metadata := make(map[string]string, len(values))
	for key, value := range values {
		text, ok := value.(string)
		if !ok {
			return nil, runtime.ErrInvariantViolation
		}
		metadata[key] = text
	}
	return metadata, nil
}
func text(payload map[string]any, key string) (string, bool) {
	value, ok := payload[key].(string)
	return value, ok && value != ""
}
func int64Value(payload map[string]any, key string) (int64, bool) {
	switch value := payload[key].(type) {
	case float64:
		return int64(value), value == math.Trunc(value)
	case json.Number:
		parsed, err := value.Int64()
		return parsed, err == nil
	case int64:
		return value, true
	case int:
		return int64(value), true
	default:
		return 0, false
	}
}

var _ knowledgedriver.SnapshotReader = (*Adapter)(nil)
var _ knowledgedriver.ReplicaWriter = (*Adapter)(nil)
var _ knowledgedriver.BackfillSource = (*Adapter)(nil)
var _ knowledgedriver.Inventory = (*Adapter)(nil)
var _ knowledgedriver.SearchTarget = (*Adapter)(nil)
