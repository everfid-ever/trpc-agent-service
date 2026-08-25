// Package secrets defines least-privilege secret resolution.
package secrets

import "context"

type SecretRef struct {
	Ref     string `json:"ref"`
	Version int64  `json:"version"`
}
type SecretValue struct {
	Bytes   []byte
	Version int64
}

type Purpose string

const (
	PurposeChannelVerify  Purpose = "channel_verify"
	PurposeChannelSend    Purpose = "channel_send"
	PurposeModelCall      Purpose = "model_call"
	PurposeToolCall       Purpose = "tool_call"
	PurposeBackendConnect Purpose = "backend_connect"
)

type Scope struct {
	TenantID        string
	Subject         string
	Purpose         Purpose
	ResourceID      string
	ResourceVersion int64
}

type Provider interface {
	Resolve(context.Context, Scope, SecretRef) (SecretValue, error)
}
type SecretProvider = Provider
