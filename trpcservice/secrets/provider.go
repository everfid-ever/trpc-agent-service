// Package secrets defines least-privilege secret resolution.
package secrets

import "context"

type SecretRef struct {
	Ref     string
	Version int64
}
type SecretValue struct {
	Bytes   []byte
	Version int64
}
type Provider interface {
	Resolve(context.Context, SecretRef) (SecretValue, error)
}
type SecretProvider = Provider
