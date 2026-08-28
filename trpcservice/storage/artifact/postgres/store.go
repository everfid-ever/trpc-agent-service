// Package postgres implements the durable tenant-scoped media ArtifactStore.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) PutArtifact(ctx context.Context, in artifact.Record) (artifact.Record, error) {
	if s == nil || s.db == nil {
		return artifact.Record{}, runtime.ErrCapabilityUnsupported
	}
	id, ref, err := artifact.StableIdentity(in.TenantID, in.RequestID, in.Ordinal, in.SourceDigest)
	if err != nil {
		return artifact.Record{}, err
	}
	contentDigest := sha256.Sum256(in.Content)
	if in.ArtifactID != id || in.ArtifactRef != ref || in.ContentDigest != hex.EncodeToString(contentDigest[:]) || in.MediaType == "" ||
		(in.Kind != "image" && in.Kind != "file") || len(in.Content) == 0 || in.MalwareScanVersion == "" || in.DLPVersion == "" {
		return artifact.Record{}, runtime.ErrInvalidEnvelope
	}
	var stored artifact.Record
	err = s.db.QueryRowContext(ctx, `INSERT INTO media_artifact(
tenant_id,request_id,artifact_id,artifact_ref,ordinal,source_digest,content_digest,media_type,kind,content,malware_scan_version,dlp_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (tenant_id,artifact_id) DO UPDATE SET artifact_id=EXCLUDED.artifact_id
RETURNING tenant_id,request_id,artifact_id,artifact_ref,ordinal,source_digest,content_digest,media_type,kind,content,malware_scan_version,dlp_version,created_at`,
		in.TenantID, in.RequestID, in.ArtifactID, in.ArtifactRef, in.Ordinal, in.SourceDigest, in.ContentDigest, in.MediaType, in.Kind,
		in.Content, in.MalwareScanVersion, in.DLPVersion).
		Scan(&stored.TenantID, &stored.RequestID, &stored.ArtifactID, &stored.ArtifactRef, &stored.Ordinal, &stored.SourceDigest,
			&stored.ContentDigest, &stored.MediaType, &stored.Kind, &stored.Content, &stored.MalwareScanVersion, &stored.DLPVersion, &stored.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return artifact.Record{}, runtime.ErrVersionConflict
	}
	if err != nil {
		return artifact.Record{}, translate(err)
	}
	if stored.RequestID != in.RequestID || stored.ArtifactRef != in.ArtifactRef || stored.Ordinal != in.Ordinal || stored.SourceDigest != in.SourceDigest ||
		stored.ContentDigest != in.ContentDigest || stored.MediaType != in.MediaType || stored.Kind != in.Kind || stored.MalwareScanVersion != in.MalwareScanVersion ||
		stored.DLPVersion != in.DLPVersion || !bytes.Equal(stored.Content, in.Content) {
		return artifact.Record{}, runtime.ErrIdempotencyCollision
	}
	return stored, nil
}

func (s *Store) GetArtifact(ctx context.Context, tenantID, artifactID string) (artifact.Record, error) {
	if s == nil || s.db == nil {
		return artifact.Record{}, runtime.ErrCapabilityUnsupported
	}
	if tenantID == "" || artifactID == "" {
		return artifact.Record{}, runtime.ErrTenantScope
	}
	var stored artifact.Record
	err := s.db.QueryRowContext(ctx, `SELECT tenant_id,request_id,artifact_id,artifact_ref,ordinal,source_digest,content_digest,media_type,kind,content,malware_scan_version,dlp_version,created_at
FROM media_artifact WHERE tenant_id=$1 AND artifact_id=$2`, tenantID, artifactID).
		Scan(&stored.TenantID, &stored.RequestID, &stored.ArtifactID, &stored.ArtifactRef, &stored.Ordinal, &stored.SourceDigest,
			&stored.ContentDigest, &stored.MediaType, &stored.Kind, &stored.Content, &stored.MalwareScanVersion, &stored.DLPVersion, &stored.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return artifact.Record{}, runtime.ErrNotFound
	}
	if err != nil {
		return artifact.Record{}, translate(err)
	}
	return stored, nil
}

type sqlStater interface{ SQLState() string }

func translate(err error) error {
	if err == nil {
		return nil
	}
	var state sqlStater
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "23505":
			return runtime.ErrIdempotencyCollision
		case "23503", "23514":
			return runtime.ErrInvalidEnvelope
		case "40001":
			return runtime.ErrVersionConflict
		}
	}
	return err
}

var _ artifact.Store = (*Store)(nil)
