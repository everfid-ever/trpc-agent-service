package profile

import "context"

// RuntimeBundle is deliberately opaque to shared contracts. Concrete Workers
// type-assert only capabilities owned by their bundle factory.
type RuntimeBundle interface{}

type BundleLease interface {
	Bundle() RuntimeBundle
	Release()
}

type RuntimeBundleManager interface {
	Acquire(context.Context, ExecutionProfileKey) (BundleLease, error)
	Retire(ExecutionProfileKey)
	Close(context.Context) error
}
