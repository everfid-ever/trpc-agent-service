// Package memory defines the tenant-scoped long-term memory port. Memory is
// deliberately separate from Session: it has eventual index visibility and a
// request-local RYW overlay, while Session remains the turn transaction root.
package memory

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound        = errors.New("memory entry not found")
	ErrInvalid         = errors.New("invalid memory entry")
	ErrVersionConflict = errors.New("memory version conflict")
	ErrTenantScope     = errors.New("memory tenant scope mismatch")
)

type Scope string

const (
	ScopeTenant Scope = "tenant"
	ScopeUser   Scope = "user"
)

type Key struct {
	TenantID  string
	Scope     Scope
	SubjectID string
	MemoryID  string
}

type Entry struct {
	Key
	Version         int64
	TenantWatermark int64
	ContentRef      string
	ContentDigest   string
	Attributes      map[string]string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type PutRequest struct {
	Entry
	ExpectedVersion int64
	MutationID      string
}

type Query struct {
	TenantID  string
	Scope     Scope
	SubjectID string
	Limit     int
}

type Invalidation struct {
	TenantID string
	Version  int64
}

type Store interface {
	Put(context.Context, PutRequest) (Entry, error)
	Get(context.Context, Key) (Entry, error)
	List(context.Context, Query) ([]Entry, error)
	TenantWatermark(context.Context, string) (int64, error)
}

func (k Key) Validate() error {
	if k.TenantID == "" || k.MemoryID == "" {
		return ErrInvalid
	}
	switch k.Scope {
	case ScopeTenant:
		if k.SubjectID != "" {
			return ErrInvalid
		}
	case ScopeUser:
		if k.SubjectID == "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

func (e Entry) Validate() error {
	if err := e.Key.Validate(); err != nil || e.Version < 1 || e.TenantWatermark < 1 || e.ContentRef == "" || len(e.ContentDigest) != 64 {
		return ErrInvalid
	}
	return nil
}
