package agentapp

import "testing"

func TestContentDigestNormalizesReferenceOrder(t *testing.T) {
	base := Revision{AgentKind: "llm", SchemaVersion: 1, Instruction: "help", ModelProfileID: "model", ModelProfileVersion: 1, ToolRefs: []VersionedRef{{ID: "b", Version: 1}, {ID: "a", Version: 1}}}
	a, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	base.ToolRefs[0], base.ToolRefs[1] = base.ToolRefs[1], base.ToolRefs[0]
	b, err := base.ComputeContentDigest()
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("digest changed with reference order: %s != %s", a, b)
	}
}
