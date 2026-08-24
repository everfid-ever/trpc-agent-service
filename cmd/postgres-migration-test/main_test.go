package main

import (
	"strings"
	"testing"
)

func TestTestDSNForDatabaseOverridesKeywordDatabase(t *testing.T) {
	dsn, err := testDSNForDatabase("host=/tmp/trpc-pg16 port=55432 user=postgres dbname=postgres sslmode=verify-full application_name=matrix", "trpc_agent_service_test_0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "dbname='trpc_agent_service_test_0123456789abcdef'") {
		t.Fatalf("missing test database in DSN: %s", dsn)
	}
	if !strings.Contains(dsn, "dbname=postgres") {
		t.Fatalf("admin database should remain visible before override for auditability: %s", dsn)
	}
	if !strings.Contains(dsn, "sslmode=verify-full") || !strings.Contains(dsn, "application_name=matrix") {
		t.Fatalf("connection settings were not preserved: %s", dsn)
	}
}

func TestTestDSNForDatabaseOverridesURLDatabase(t *testing.T) {
	dsn, err := testDSNForDatabase("postgres://user:pass@example.com:5432/postgres?sslmode=require", "trpc_agent_service_test_0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if dsn != "postgres://user:pass@example.com:5432/trpc_agent_service_test_0123456789abcdef?sslmode=require" {
		t.Fatalf("test DSN=%s", dsn)
	}
}

func TestQuoteConninfoValue(t *testing.T) {
	if got := quoteConninfoValue(`dir/with'\slash`); got != `'dir/with\'\\slash'` {
		t.Fatalf("quoteConninfoValue()=%s", got)
	}
}
