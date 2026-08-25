package postgres

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestPayloadEncryptionRoundTripAndAADBinding(t *testing.T) {
	key := bytes.Repeat([]byte{0x3c}, 32)
	aad := []byte("tenant\x00request\x00ref\x00digest")
	plaintext := []byte(`{"text":"secret"}`)
	ciphertext, nonce, err := encryptPayload(key, aad, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("ciphertext contains plaintext")
	}
	actual, err := decryptPayload(key, aad, ciphertext, nonce)
	if err != nil || !bytes.Equal(actual, plaintext) {
		t.Fatalf("actual=%q err=%v", actual, err)
	}
	if _, err := decryptPayload(key, []byte("other scope"), ciphertext, nonce); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("wrong AAD err=%v", err)
	}
}

func TestPostgreSQLPayloadStoreWithoutKeyFailsClosed(t *testing.T) {
	store := New(nil)
	err := store.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: "tenant", RequestID: "request", PayloadRef: "payload://request", ContentDigest: string(bytes.Repeat([]byte{'a'}, 64)), Content: []byte("payload"), KeyVersion: 1})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("err=%v", err)
	}
}
