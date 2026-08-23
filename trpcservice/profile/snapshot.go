// Package profile resolves immutable control-plane versions into executable
// runtime snapshots.
package profile

type VersionedRef struct {
	ID      string
	Version int64
}

type CapabilitySet map[string]bool
type GenerationConfigV1 map[string]any
type RuntimePolicyV1 map[string]any

type ExecutionProfileKey struct {
	TenantID         string
	AgentAppID       string
	AgentAppRevision int64
	ContentDigest    string
	ConfigVersion    int64
	PolicyVersion    int64
}

type ExecutionProfileSnapshot struct {
	Key                 ExecutionProfileKey
	TenantVersion       int64
	AgentAppVersion     int64
	ContentDigest       string
	AgentKind           string
	Instruction         string
	GlobalInstruction   string
	ModelProfileRef     VersionedRef
	ToolRefs            []VersionedRef
	KnowledgeRefs       []VersionedRef
	GenerationConfig    GenerationConfigV1
	RuntimePolicy       RuntimePolicyV1
	BackendRequirements CapabilitySet
}
