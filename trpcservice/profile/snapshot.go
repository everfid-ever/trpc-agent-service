// Package profile resolves immutable control-plane versions into executable
// runtime snapshots.
package profile

import "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"

type VersionedRef struct {
	ID      string
	Version int64
}

type SkillRef = agentapp.SkillRef
type AgentSpecV1 = agentapp.AgentSpecV1

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
	AppName             string
	AgentKind           agentapp.AgentKind
	AgentSpec           AgentSpecV1
	Description         string
	Instruction         string
	GlobalInstruction   string
	ModelProfileRef     VersionedRef
	ToolRefs            []VersionedRef
	SkillRefs           []SkillRef
	KnowledgeRefs       []VersionedRef
	GenerationConfig    GenerationConfigV1
	RuntimePolicy       RuntimePolicyV1
	BackendRequirements CapabilitySet
}
