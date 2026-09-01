package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type Source struct{ db *sql.DB }

func NewSource(db *sql.DB) *Source { return &Source{db: db} }

type keyCursor struct {
	AgentAppID string `json:"agent_app_id"`
	SessionID  string `json:"session_id"`
	Empty      bool   `json:"empty,omitempty"`
}

func (s *Source) CaptureWatermark(ctx context.Context, tenantID string) (string, error) {
	if s == nil || s.db == nil {
		return "", runtime.ErrBackendUnavailable
	}
	if tenantID == "" {
		return "", runtime.ErrTenantScope
	}
	var cursor keyCursor
	err := s.db.QueryRowContext(ctx, `SELECT agent_app_id,session_id FROM public.session_head
WHERE tenant_id=$1 ORDER BY agent_app_id DESC,session_id DESC LIMIT 1`, tenantID).
		Scan(&cursor.AgentAppID, &cursor.SessionID)
	if errors.Is(err, sql.ErrNoRows) {
		cursor.Empty = true
		return encodeCursor(cursor)
	}
	if err != nil {
		return "", err
	}
	return encodeCursor(cursor)
}

func (s *Source) LoadSessionImage(ctx context.Context, key sessionstore.SessionKey) (sessiondriver.SessionImage, error) {
	if s == nil || s.db == nil {
		return sessiondriver.SessionImage{}, runtime.ErrBackendUnavailable
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	defer tx.Rollback()
	image, err := loadImage(ctx, tx, key)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	if err := tx.Commit(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	return image, nil
}

func (s *Source) PageSessions(ctx context.Context, in sessiondriver.PageRequest) (sessiondriver.Page, error) {
	if s == nil || s.db == nil {
		return sessiondriver.Page{}, runtime.ErrBackendUnavailable
	}
	if in.TenantID == "" || in.Limit < 1 || in.Limit > 1000 {
		return sessiondriver.Page{}, runtime.ErrInvariantViolation
	}
	upper, err := decodeCursor(in.SnapshotWatermark)
	if err != nil {
		return sessiondriver.Page{}, err
	}
	if upper.Empty {
		return sessiondriver.Page{NextCheckpoint: eofCheckpoint(in.SnapshotWatermark), Complete: true}, nil
	}
	after := keyCursor{}
	if in.After != "" {
		after, err = decodeCursor(in.After)
		if err != nil || after.Empty {
			return sessiondriver.Page{}, runtime.ErrInvariantViolation
		}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return sessiondriver.Page{}, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT agent_app_id,session_id FROM public.session_head
WHERE tenant_id=$1 AND (agent_app_id,session_id)>($2,$3) AND (agent_app_id,session_id)<=($4,$5)
ORDER BY agent_app_id,session_id LIMIT $6`, in.TenantID, after.AgentAppID, after.SessionID,
		upper.AgentAppID, upper.SessionID, in.Limit+1)
	if err != nil {
		return sessiondriver.Page{}, err
	}
	var keys []sessionstore.SessionKey
	for rows.Next() {
		key := sessionstore.SessionKey{TenantID: in.TenantID}
		if err := rows.Scan(&key.AgentAppID, &key.SessionID); err != nil {
			rows.Close()
			return sessiondriver.Page{}, err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return sessiondriver.Page{}, err
	}
	complete := len(keys) <= in.Limit
	if len(keys) > in.Limit {
		keys = keys[:in.Limit]
	}
	page := sessiondriver.Page{Complete: complete}
	for _, key := range keys {
		image, err := loadImage(ctx, tx, key)
		if err != nil {
			return sessiondriver.Page{}, err
		}
		page.Sessions = append(page.Sessions, image)
	}
	if len(keys) == 0 {
		page.NextCheckpoint = eofCheckpoint(in.SnapshotWatermark)
	} else {
		last := keys[len(keys)-1]
		page.NextCheckpoint, err = encodeCursor(keyCursor{AgentAppID: last.AgentAppID, SessionID: last.SessionID})
		if err != nil {
			return sessiondriver.Page{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return sessiondriver.Page{}, err
	}
	return page, nil
}

func (s *Source) Fingerprint(ctx context.Context, tenantID, watermark string) (sessiondriver.Fingerprint, error) {
	if watermark == "" {
		var err error
		watermark, err = s.CaptureWatermark(ctx, tenantID)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
	}
	upper, err := decodeCursor(watermark)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	if upper.Empty {
		return sessiondriver.FingerprintFromItems(nil, watermark), nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT agent_app_id,session_id FROM public.session_head
WHERE tenant_id=$1 AND (agent_app_id,session_id)<=($2,$3) ORDER BY agent_app_id,session_id`,
		tenantID, upper.AgentAppID, upper.SessionID)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	var keys []sessionstore.SessionKey
	for rows.Next() {
		key := sessionstore.SessionKey{TenantID: tenantID}
		if err := rows.Scan(&key.AgentAppID, &key.SessionID); err != nil {
			rows.Close()
			return sessiondriver.Fingerprint{}, err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	images := make([]sessiondriver.SessionImage, 0, len(keys))
	for _, key := range keys {
		image, err := loadImage(ctx, tx, key)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
		images = append(images, image)
	}
	result, err := FingerprintImages(images, watermark)
	if err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	if err := tx.Commit(); err != nil {
		return sessiondriver.Fingerprint{}, err
	}
	return result, nil
}

func FingerprintImages(images []sessiondriver.SessionImage, watermark string) (sessiondriver.Fingerprint, error) {
	items := make([]string, 0, len(images))
	for _, image := range images {
		digest, err := sessiondriver.SnapshotDigest(image)
		if err != nil {
			return sessiondriver.Fingerprint{}, err
		}
		items = append(items, image.Head.AgentAppID+"\x00"+image.Head.SessionID+"\x00"+digest)
	}
	return sessiondriver.FingerprintFromItems(items, watermark), nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadImage(ctx context.Context, q queryer, key sessionstore.SessionKey) (sessiondriver.SessionImage, error) {
	if key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" {
		return sessiondriver.SessionImage{}, runtime.ErrTenantScope
	}
	image := sessiondriver.SessionImage{}
	image.Head.SessionKey = key
	var state []byte
	var summaryID sql.NullString
	err := q.QueryRowContext(ctx, `SELECT version,last_fence,last_session_seq,next_input_seq,last_allocated_input_seq,state_json,summary_id
FROM public.session_head WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3`,
		key.TenantID, key.AgentAppID, key.SessionID).Scan(&image.Head.Version, &image.Head.LastFence,
		&image.Head.LastSessionSeq, &image.Head.NextInputSeq, &image.LastAllocatedInputSeq, &state, &summaryID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessiondriver.SessionImage{}, runtime.ErrNotFound
	}
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	if json.Unmarshal(state, &image.Head.State) != nil {
		return sessiondriver.SessionImage{}, runtime.ErrInvariantViolation
	}
	image.SummaryID = summaryID.String
	eventRows, err := q.QueryContext(ctx, `SELECT session_seq,request_id,input_seq,event_seq,event_id,event_type,
payload_ref,COALESCE(event_payload,'null'::jsonb),created_at FROM public.session_event
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 ORDER BY session_seq`, key.TenantID, key.AgentAppID, key.SessionID)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	for eventRows.Next() {
		var item sessiondriver.EventRecord
		if err := eventRows.Scan(&item.SessionSeq, &item.RequestID, &item.InputSeq, &item.EventSeq,
			&item.EventID, &item.EventType, &item.PayloadRef, &item.Payload, &item.CreatedAt); err != nil {
			eventRows.Close()
			return sessiondriver.SessionImage{}, err
		}
		item.CreatedAt = item.CreatedAt.UTC()
		image.Events = append(image.Events, item)
	}
	if err := eventRows.Close(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	commitRows, err := q.QueryContext(ctx, `SELECT commit_id,request_id,request_digest,input_seq,stage,outcome,fence,
session_version,COALESCE(reply_cursor,''),COALESCE(result_ref,''),created_at FROM public.session_commit
WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 ORDER BY session_version,commit_id`, key.TenantID, key.AgentAppID, key.SessionID)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	for commitRows.Next() {
		var item sessiondriver.CommitRecord
		if err := commitRows.Scan(&item.CommitID, &item.RequestID, &item.RequestDigest, &item.InputSeq,
			&item.Stage, &item.Outcome, &item.Fence, &item.SessionVersion, &item.ReplyCursor,
			&item.ResultRef, &item.CreatedAt); err != nil {
			commitRows.Close()
			return sessiondriver.SessionImage{}, err
		}
		item.CreatedAt = item.CreatedAt.UTC()
		image.Commits = append(image.Commits, item)
	}
	if err := commitRows.Close(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	summaryRows, err := q.QueryContext(ctx, `SELECT summary_id,base_session_seq,last_event_id,cutoff_at,content_ref,created_at
FROM public.session_summary WHERE tenant_id=$1 AND agent_app_id=$2 AND session_id=$3 ORDER BY base_session_seq`,
		key.TenantID, key.AgentAppID, key.SessionID)
	if err != nil {
		return sessiondriver.SessionImage{}, err
	}
	for summaryRows.Next() {
		var item sessiondriver.SummaryRecord
		if err := summaryRows.Scan(&item.SummaryID, &item.BaseSessionSeq, &item.LastEventID,
			&item.CutoffAt, &item.ContentRef, &item.CreatedAt); err != nil {
			summaryRows.Close()
			return sessiondriver.SessionImage{}, err
		}
		item.CutoffAt, item.CreatedAt = item.CutoffAt.UTC(), item.CreatedAt.UTC()
		image.Summaries = append(image.Summaries, item)
	}
	if err := summaryRows.Close(); err != nil {
		return sessiondriver.SessionImage{}, err
	}
	return image, nil
}

func encodeCursor(value keyCursor) (string, error) {
	return sessiondriver.EncodeSessionWatermark(value.AgentAppID, value.SessionID, value.Empty)
}

func decodeCursor(value string) (keyCursor, error) {
	agentAppID, sessionID, empty, err := sessiondriver.DecodeSessionWatermark(value)
	if err != nil {
		return keyCursor{}, err
	}
	return keyCursor{AgentAppID: agentAppID, SessionID: sessionID, Empty: empty}, nil
}

func eofCheckpoint(watermark string) string {
	return "pg-session-eof-v1:" + sessiondriver.FingerprintFromItems([]string{watermark}, watermark).Digest
}

var _ sessiondriver.SnapshotReader = (*Source)(nil)
var _ sessiondriver.BackfillSource = (*Source)(nil)
var _ sessiondriver.Inventory = (*Source)(nil)
