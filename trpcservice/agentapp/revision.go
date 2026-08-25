package agentapp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"time"
)

var agentNodeKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

const FailurePolicyFailFast = "fail_fast"

type RevisionState string
type AgentKind string

const (
	RevisionDraft     RevisionState = "draft"
	RevisionPublished RevisionState = "published"

	AgentKindLLM      AgentKind = "llm"
	AgentKindGraph    AgentKind = "graph"
	AgentKindChain    AgentKind = "chain"
	AgentKindParallel AgentKind = "parallel"
	AgentKindCycle    AgentKind = "cycle"
)

type VersionedRef struct {
	ID       string `json:"id"`
	Version  int64  `json:"version"`
	Required bool   `json:"required,omitempty"`
}

type SkillRef struct {
	ID            string `json:"id"`
	Version       int64  `json:"version"`
	ContentDigest string `json:"content_digest"`
}

type PublishedAgentRef struct {
	AgentAppID    string `json:"agent_app_id"`
	Revision      int64  `json:"revision"`
	ContentDigest string `json:"content_digest"`
}

type AgentNodeSpecV1 struct {
	Key           string            `json:"key"`
	AgentRef      PublishedAgentRef `json:"agent_ref"`
	FailurePolicy string            `json:"failure_policy"`
}

type AgentEdgeSpecV1 struct {
	From         string        `json:"from"`
	To           string        `json:"to"`
	ConditionRef *VersionedRef `json:"condition_ref,omitempty"`
}

type CheckpointPolicyV1 struct {
	Required  bool   `json:"required"`
	Namespace string `json:"namespace,omitempty"`
}

type AgentSpecV1 struct {
	Nodes          []AgentNodeSpecV1  `json:"nodes,omitempty"`
	Edges          []AgentEdgeSpecV1  `json:"edges,omitempty"`
	EntryNode      string             `json:"entry_node,omitempty"`
	MaxConcurrency int                `json:"max_concurrency,omitempty"`
	MaxIterations  int                `json:"max_iterations,omitempty"`
	Checkpoint     CheckpointPolicyV1 `json:"checkpoint"`
}

type Revision struct {
	TenantID            string
	AgentAppID          string
	Revision            int64
	State               RevisionState
	DraftVersion        int64
	AgentKind           AgentKind
	SchemaVersion       int
	AgentSpec           AgentSpecV1
	Description         string
	Instruction         string
	GlobalInstruction   string
	ModelProfileID      string
	ModelProfileVersion int64
	ToolRefs            []VersionedRef
	SkillRefs           []SkillRef
	KnowledgeRefs       []VersionedRef
	GenerationConfig    map[string]any
	RuntimePolicy       map[string]any
	ContentDigest       string
	PublishedAt         *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (r Revision) ValidateDraft() error {
	if r.TenantID == "" || r.AgentAppID == "" || r.Revision < 1 ||
		r.DraftVersion < 1 || r.SchemaVersion != 1 {
		return fmt.Errorf("%w: incomplete or unsupported revision", ErrInvalid)
	}
	if !isSupportedAgentKind(r.AgentKind) {
		return fmt.Errorf("%w: unsupported agent kind %q", ErrInvalid, r.AgentKind)
	}
	if r.AgentKind == AgentKindLLM {
		if r.Instruction == "" || r.ModelProfileID == "" || r.ModelProfileVersion < 1 || !r.AgentSpec.empty() {
			return fmt.Errorf("%w: invalid llm revision", ErrInvalid)
		}
	} else {
		if r.ModelProfileID != "" || r.ModelProfileVersion != 0 {
			return fmt.Errorf("%w: composite agent cannot own a model profile", ErrInvalid)
		}
		if err := r.AgentSpec.validate(r.AgentKind); err != nil {
			return err
		}
	}
	for kind, refs := range map[string][]VersionedRef{"tool": r.ToolRefs, "knowledge": r.KnowledgeRefs} {
		seen := make(map[string]struct{}, len(refs))
		for _, ref := range refs {
			if ref.ID == "" || ref.Version < 1 {
				return fmt.Errorf("%w: invalid %s reference", ErrInvalid, kind)
			}
			if _, exists := seen[ref.ID]; exists {
				return fmt.Errorf("%w: duplicate %s reference %q", ErrInvalid, kind, ref.ID)
			}
			seen[ref.ID] = struct{}{}
		}
	}
	seenSkills := make(map[string]struct{}, len(r.SkillRefs))
	for _, ref := range r.SkillRefs {
		if ref.ID == "" || ref.Version < 1 || !isDigest(ref.ContentDigest) {
			return fmt.Errorf("%w: invalid skill reference", ErrInvalid)
		}
		if _, exists := seenSkills[ref.ID]; exists {
			return fmt.Errorf("%w: duplicate skill reference %q", ErrInvalid, ref.ID)
		}
		seenSkills[ref.ID] = struct{}{}
	}
	return nil
}

func isSupportedAgentKind(kind AgentKind) bool {
	switch kind {
	case AgentKindLLM, AgentKindGraph, AgentKindChain, AgentKindParallel, AgentKindCycle:
		return true
	default:
		return false
	}
}

func isDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s AgentSpecV1) empty() bool {
	return len(s.Nodes) == 0 && len(s.Edges) == 0 && s.EntryNode == "" &&
		s.MaxConcurrency == 0 && s.MaxIterations == 0 && !s.Checkpoint.Required && s.Checkpoint.Namespace == ""
}

func (s AgentSpecV1) validate(kind AgentKind) error {
	if len(s.Nodes) == 0 || len(s.Nodes) > 256 {
		return fmt.Errorf("%w: invalid agent node count", ErrInvalid)
	}
	seen := make(map[string]struct{}, len(s.Nodes))
	for _, node := range s.Nodes {
		if !agentNodeKeyPattern.MatchString(node.Key) || node.FailurePolicy != FailurePolicyFailFast ||
			node.AgentRef.AgentAppID == "" || node.AgentRef.Revision < 1 ||
			!isDigest(node.AgentRef.ContentDigest) {
			return fmt.Errorf("%w: invalid agent node", ErrInvalid)
		}
		if _, exists := seen[node.Key]; exists {
			return fmt.Errorf("%w: duplicate agent node %q", ErrInvalid, node.Key)
		}
		seen[node.Key] = struct{}{}
	}
	switch kind {
	case AgentKindGraph:
		if _, ok := seen[s.EntryNode]; !ok || s.MaxConcurrency < 1 || s.MaxConcurrency > 128 || s.MaxIterations != 0 ||
			s.Checkpoint.Required != (s.Checkpoint.Namespace != "") {
			return fmt.Errorf("%w: invalid graph limits or entry", ErrInvalid)
		}
		seenEdges := make(map[string]struct{}, len(s.Edges))
		for _, edge := range s.Edges {
			if edge.From == "" || edge.To == "" {
				return fmt.Errorf("%w: invalid graph edge", ErrInvalid)
			}
			if _, ok := seen[edge.From]; !ok {
				return fmt.Errorf("%w: graph edge source does not exist", ErrInvalid)
			}
			if _, ok := seen[edge.To]; !ok {
				return fmt.Errorf("%w: graph edge target does not exist", ErrInvalid)
			}
			key := edge.From + "\x00" + edge.To
			if _, exists := seenEdges[key]; exists {
				return fmt.Errorf("%w: duplicate graph edge", ErrInvalid)
			}
			seenEdges[key] = struct{}{}
		}
	case AgentKindChain:
		if len(s.Edges) != 0 || s.EntryNode != "" || s.MaxConcurrency != 0 || s.MaxIterations != 0 || s.Checkpoint.Required {
			return fmt.Errorf("%w: invalid chain spec", ErrInvalid)
		}
	case AgentKindParallel:
		if len(s.Edges) != 0 || s.EntryNode != "" || s.MaxConcurrency < 1 || s.MaxConcurrency > 128 ||
			s.MaxConcurrency < len(s.Nodes) || s.MaxIterations != 0 || s.Checkpoint.Required {
			return fmt.Errorf("%w: invalid parallel spec", ErrInvalid)
		}
	case AgentKindCycle:
		if len(s.Edges) != 0 || s.EntryNode != "" || s.MaxConcurrency != 0 ||
			s.MaxIterations < 1 || s.MaxIterations > 1000 || s.Checkpoint.Required {
			return fmt.Errorf("%w: invalid cycle spec", ErrInvalid)
		}
	}
	return nil
}

// NormalizeRevision returns a detached revision whose JSON object fields are
// always objects. PostgreSQL rejects JSON null for these columns, so all
// repository implementations apply the same canonical representation.
func NormalizeRevision(r Revision) Revision {
	r = cloneRevision(r)
	for index := range r.AgentSpec.Nodes {
		if r.AgentSpec.Nodes[index].FailurePolicy == "" {
			r.AgentSpec.Nodes[index].FailurePolicy = FailurePolicyFailFast
		}
	}
	if r.AgentSpec.Nodes == nil {
		r.AgentSpec.Nodes = []AgentNodeSpecV1{}
	}
	if r.AgentSpec.Edges == nil {
		r.AgentSpec.Edges = []AgentEdgeSpecV1{}
	}
	if r.GenerationConfig == nil {
		r.GenerationConfig = map[string]any{}
	}
	if r.RuntimePolicy == nil {
		r.RuntimePolicy = map[string]any{}
	}
	return r
}

// ComputeContentDigest hashes the normalized behavior-affecting definition.
func (r Revision) ComputeContentDigest() (string, error) {
	r = NormalizeRevision(r)
	tools := cloneRefs(r.ToolRefs)
	skills := append([]SkillRef(nil), r.SkillRefs...)
	knowledge := cloneRefs(r.KnowledgeRefs)
	sort.Slice(tools, func(i, j int) bool {
		if tools[i].ID == tools[j].ID {
			return tools[i].Version < tools[j].Version
		}
		return tools[i].ID < tools[j].ID
	})
	sort.Slice(knowledge, func(i, j int) bool {
		if knowledge[i].ID == knowledge[j].ID {
			return knowledge[i].Version < knowledge[j].Version
		}
		return knowledge[i].ID < knowledge[j].ID
	})
	sort.Slice(skills, func(i, j int) bool {
		if skills[i].ID == skills[j].ID {
			return skills[i].Version < skills[j].Version
		}
		return skills[i].ID < skills[j].ID
	})
	input := struct {
		AgentKind           AgentKind
		SchemaVersion       int
		AgentSpec           AgentSpecV1
		Description         string
		Instruction         string
		GlobalInstruction   string
		ModelProfileID      string
		ModelProfileVersion int64
		ToolRefs            []VersionedRef
		SkillRefs           []SkillRef
		KnowledgeRefs       []VersionedRef
		GenerationConfig    map[string]any
		RuntimePolicy       map[string]any
	}{r.AgentKind, r.SchemaVersion, r.AgentSpec, r.Description, r.Instruction,
		r.GlobalInstruction, r.ModelProfileID, r.ModelProfileVersion,
		tools, skills, knowledge, r.GenerationConfig, r.RuntimePolicy}
	b, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("digest revision: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func cloneRevision(r Revision) Revision {
	r.ToolRefs = cloneRefs(r.ToolRefs)
	r.SkillRefs = append([]SkillRef(nil), r.SkillRefs...)
	r.KnowledgeRefs = cloneRefs(r.KnowledgeRefs)
	r.AgentSpec = cloneAgentSpec(r.AgentSpec)
	r.GenerationConfig = cloneMap(r.GenerationConfig)
	r.RuntimePolicy = cloneMap(r.RuntimePolicy)
	if r.PublishedAt != nil {
		v := *r.PublishedAt
		r.PublishedAt = &v
	}
	return r
}

func cloneAgentSpec(in AgentSpecV1) AgentSpecV1 {
	b, _ := json.Marshal(in)
	var out AgentSpecV1
	_ = json.Unmarshal(b, &out)
	return out
}

func cloneRefs(in []VersionedRef) []VersionedRef { return append([]VersionedRef(nil), in...) }

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	b, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out
}
