package agentapp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

type RevisionState string

const (
	RevisionDraft     RevisionState = "draft"
	RevisionPublished RevisionState = "published"
)

type VersionedRef struct {
	ID       string `json:"id"`
	Version  int64  `json:"version"`
	Required bool   `json:"required,omitempty"`
}

type Revision struct {
	TenantID            string
	AgentAppID          string
	Revision            int64
	State               RevisionState
	DraftVersion        int64
	AgentKind           string
	SchemaVersion       int
	Description         string
	Instruction         string
	GlobalInstruction   string
	ModelProfileID      string
	ModelProfileVersion int64
	ToolRefs            []VersionedRef
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
		r.DraftVersion < 1 || r.AgentKind != "llm" || r.SchemaVersion != 1 ||
		r.Instruction == "" || r.ModelProfileID == "" || r.ModelProfileVersion < 1 {
		return fmt.Errorf("%w: incomplete or unsupported revision", ErrInvalid)
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
	return nil
}

// ComputeContentDigest hashes the normalized behavior-affecting definition.
func (r Revision) ComputeContentDigest() (string, error) {
	tools := cloneRefs(r.ToolRefs)
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
	input := struct {
		AgentKind           string
		SchemaVersion       int
		Description         string
		Instruction         string
		GlobalInstruction   string
		ModelProfileID      string
		ModelProfileVersion int64
		ToolRefs            []VersionedRef
		KnowledgeRefs       []VersionedRef
		GenerationConfig    map[string]any
		RuntimePolicy       map[string]any
	}{r.AgentKind, r.SchemaVersion, r.Description, r.Instruction,
		r.GlobalInstruction, r.ModelProfileID, r.ModelProfileVersion,
		tools, knowledge, r.GenerationConfig, r.RuntimePolicy}
	b, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("digest revision: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func cloneRevision(r Revision) Revision {
	r.ToolRefs = cloneRefs(r.ToolRefs)
	r.KnowledgeRefs = cloneRefs(r.KnowledgeRefs)
	r.GenerationConfig = cloneMap(r.GenerationConfig)
	r.RuntimePolicy = cloneMap(r.RuntimePolicy)
	if r.PublishedAt != nil {
		v := *r.PublishedAt
		r.PublishedAt = &v
	}
	return r
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
