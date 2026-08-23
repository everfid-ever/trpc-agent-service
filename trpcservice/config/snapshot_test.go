package config

import "testing"

func TestDecodeRejectsUnknownFieldsAndTrailingData(t *testing.T) {
	for _, data := range []string{`{"schema_version":1,"default_agent_app_id":"app","policy_version":1,"unknown":true}`, `{"schema_version":1,"default_agent_app_id":"app","policy_version":1}{}`} {
		if _, err := DecodeV1([]byte(data)); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}
func TestDigestNormalizesBindingOrder(t *testing.T) {
	first := ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: 1, ChannelBindings: []ChannelBinding{{BindingID: "b"}, {BindingID: "a"}}}
	second := first
	second.ChannelBindings = append([]ChannelBinding(nil), first.ChannelBindings...)
	second.ChannelBindings[0], second.ChannelBindings[1] = second.ChannelBindings[1], second.ChannelBindings[0]
	a, _, err := ContentDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := ContentDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("digest mismatch %s %s", a, b)
	}
}
