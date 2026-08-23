// Package storage defines tenant-scoped backend routing contracts.
package storage

import "context"

type Domain string
type Capability string
type CapabilitySet map[Capability]bool

const (
	CapabilityAtomicTurnCommit     Capability = "atomic_turn_commit"
	CapabilityStrongReadAfterWrite Capability = "strong_ryw"
	CapabilitySummaryCAS           Capability = "summary_cas"
)

type BackendBinding struct {
	TenantID          string
	Domain            Domain
	BackendType       string
	BackendRef        string
	CredentialRef     string
	CredentialVersion int64
	Capabilities      CapabilitySet
}

type ScopedBackend interface{ TenantID() string }
type Router interface {
	Resolve(context.Context, Domain) (ScopedBackend, error)
}
