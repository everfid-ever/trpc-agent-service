package agent

import (
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestGraphConfirmationResumeMessageRequiresExactTargetedPayload(t *testing.T) {
	ask := map[string]struct{}{"danger": {}}
	resume := map[string]any{"schema_version": 1, "kind": "tool_result", "tool_call_id": "call-1",
		"tool_name": "danger", "result": `{"done":true}`}
	state := map[string]any{graph.StateKeyCommand: graph.NewResumeCommand().AddResumeValue("call-1", resume)}
	message, present, valid := graphConfirmationResumeMessage(state, ask)
	if !present || !valid || message.Role != model.RoleTool || message.ToolID != "call-1" {
		t.Fatalf("message=%#v present=%v valid=%v", message, present, valid)
	}

	resume["unexpected"] = true
	if _, present, valid := graphConfirmationResumeMessage(state, ask); !present || valid {
		t.Fatalf("unknown resume field accepted: present=%v valid=%v", present, valid)
	}
	delete(resume, "unexpected")
	resume["tool_name"] = "other"
	if _, present, valid := graphConfirmationResumeMessage(state, ask); !present || valid {
		t.Fatalf("wrong resume tool accepted: present=%v valid=%v", present, valid)
	}
}

func TestGraphConfirmationResumeMessageDistinguishesInitialRunFromMalformedResume(t *testing.T) {
	ask := map[string]struct{}{"danger": {}}
	if _, present, valid := graphConfirmationResumeMessage(nil, ask); present || valid {
		t.Fatalf("initial run treated as resume: present=%v valid=%v", present, valid)
	}
	if _, present, valid := graphConfirmationResumeMessage(map[string]any{graph.StateKeyCommand: "invalid"}, ask); !present || valid {
		t.Fatalf("malformed command did not fail closed: present=%v valid=%v", present, valid)
	}
}
