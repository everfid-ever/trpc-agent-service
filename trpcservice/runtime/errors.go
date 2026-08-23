package runtime

import "errors"

var (
	ErrUnsupportedSchema  = errors.New("unsupported schema version")
	ErrInvalidEnvelope    = errors.New("invalid execution envelope")
	ErrTenantScope        = errors.New("tenant scope mismatch")
	ErrVersionMismatch    = errors.New("fixed version mismatch")
	ErrNotFound           = errors.New("not found")
	ErrVersionConflict    = errors.New("version conflict")
	ErrStaleFence         = errors.New("stale fence")
	ErrInputNotReady      = errors.New("input is not ready")
	ErrAlreadyTerminal    = errors.New("input is already terminal")
	ErrLeaseLost          = errors.New("lease lost")
	ErrCommitConflict     = errors.New("commit conflict")
	ErrBackendUnavailable = errors.New("backend unavailable")
)
