package main

import (
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

func TestLoadSchemaMigrationConfig(t *testing.T) {
	config, err := loadSchemaMigrationConfig(mapEnvironment(map[string]string{
		"TRPC_POSTGRES_DSN":               "postgres://database/service",
		"TRPC_MIGRATION_EXPECTED_CURRENT": "000025",
		"TRPC_MIGRATION_TARGET":           "000026",
		"TRPC_MIGRATION_TIMEOUT":          "15m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if config.ExpectedCurrent != "000025" || config.Target != "000026" || config.Timeout != 15*time.Minute {
		t.Fatalf("config=%+v", config)
	}
}

func TestLoadSchemaMigrationConfigFailsClosed(t *testing.T) {
	valid := map[string]string{
		"TRPC_POSTGRES_DSN":               "postgres://database/service",
		"TRPC_MIGRATION_EXPECTED_CURRENT": "000025",
		"TRPC_MIGRATION_TARGET":           "000026",
	}
	for _, mutation := range []func(map[string]string){
		func(values map[string]string) { delete(values, "TRPC_POSTGRES_DSN") },
		func(values map[string]string) { values["TRPC_MIGRATION_EXPECTED_CURRENT"] = "latest" },
		func(values map[string]string) { values["TRPC_MIGRATION_TARGET"] = "26" },
		func(values map[string]string) { values["TRPC_MIGRATION_TARGET"] = "000025" },
		func(values map[string]string) { values["TRPC_MIGRATION_TIMEOUT"] = "2h1m" },
	} {
		values := make(map[string]string, len(valid))
		for key, value := range valid {
			values[key] = value
		}
		mutation(values)
		if _, err := loadSchemaMigrationConfig(mapEnvironment(values)); err == nil {
			t.Fatalf("expected rejection for %v", values)
		}
	}
}

func TestValidateSchemaMigrationTransition(t *testing.T) {
	already, err := validateSchemaMigrationTransition(migrations.Plan{Current: "000026", Target: "000026"}, "000025")
	if err != nil || !already {
		t.Fatalf("idempotent retry rejected: already=%v err=%v", already, err)
	}
	already, err = validateSchemaMigrationTransition(migrations.Plan{
		Current: "000025", Target: "000026", Pending: []string{"000026"},
	}, "000025")
	if err != nil || already {
		t.Fatalf("forward plan rejected: already=%v err=%v", already, err)
	}
	if _, err := validateSchemaMigrationTransition(migrations.Plan{
		Current: "000024", Target: "000026", Pending: []string{"000025", "000026"},
	}, "000025"); err == nil || !strings.Contains(err.Error(), "source mismatch") {
		t.Fatalf("expected source mismatch, got %v", err)
	}
}
