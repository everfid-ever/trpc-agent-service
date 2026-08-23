// Package governance defines fixed policy inputs and deterministic decisions.
package governance

type PolicySnapshot struct {
	TenantID string
	Version  int64
	Rules    map[string]any
}

type Decision struct {
	DecisionID    string
	TenantID      string
	RequestID     string
	Stage         string
	Action        string
	ReasonCode    string
	PolicyVersion int64
	RuleIDs       []string
	ReservationID string
}

type GovernanceDecision = Decision
