package runtime

import (
	"errors"
	"testing"
	"time"
)

func validEnvelope() ExecutionEnvelope {
	return ExecutionEnvelope{SchemaVersion: 1, TenantID: "t_01", TenantVersion: 1, AgentAppID: "app_01", AgentAppVersion: 2, AgentAppRevision: 3, AgentContentDigest: "digest", ConfigVersion: 4, PolicyVersion: 5, RequestID: "req_01", SessionID: "s_01", UserID: "u_01", Channel: "fake", InputSeq: 1, PayloadRef: "payload://1", CreatedAt: time.Unix(1, 0).UTC()}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	in := validEnvelope()
	data, err := MarshalEnvelope(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round trip mismatch: %#v", out)
	}
}
func TestEnvelopeRejectsUnknownSchema(t *testing.T) {
	in := validEnvelope()
	in.SchemaVersion = 2
	_, err := MarshalEnvelope(in)
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("got %v", err)
	}
}
func TestEnvelopeRejectsUnknownField(t *testing.T) {
	data := []byte(`{"schema_version":1,"unknown":true}`)
	if _, err := UnmarshalEnvelope(data); err == nil {
		t.Fatal("expected unknown field rejection")
	}
}
