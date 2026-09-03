// Command postgres-migration-test runs the destructive migration matrix only
// against a random database that it creates and drops itself.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		return fmt.Errorf("refusing: set TRPC_MIGRATION_TEST=1")
	}
	adminDSN := os.Getenv("TRPC_POSTGRES_ADMIN_DSN")
	if adminDSN == "" {
		return fmt.Errorf("TRPC_POSTGRES_ADMIN_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		return err
	}
	defer admin.Close()
	var major int
	if err := admin.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int/10000`).Scan(&major); err != nil {
		return err
	}
	if major != 16 {
		return fmt.Errorf("PostgreSQL 16 is required, found %d", major)
	}
	databaseName, err := randomDatabaseName()
	if err != nil {
		return err
	}
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+databaseName+`"`); err != nil {
		return fmt.Errorf("create test database: %w", err)
	}
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+databaseName+`" WITH (FORCE)`)
	}()
	testDSN, err := testDSNForDatabase(adminDSN, databaseName)
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	runner := migrations.NewRunner(db)
	probes, err := migrations.ContractProbes()
	if err != nil {
		return err
	}
	repoRoot, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(repoRoot, "go.mod")); err != nil {
		return fmt.Errorf("run from repository root: %w", err)
	}
	if err := verifyLegacyBackendUpgrade(ctx, runner, db); err != nil {
		return fmt.Errorf("legacy backend upgrade: %w", err)
	}
	if err := runner.DownAll(ctx); err != nil {
		return err
	}
	if err := verifyUp(ctx, runner, db, probes, repoRoot, testDSN); err != nil {
		return err
	}
	if err := cleanupAuditRetentionTestRole(ctx, db); err != nil {
		return fmt.Errorf("clean audit retention test role: %w", err)
	}
	if err := runner.DownAll(ctx); err != nil {
		return err
	}
	empty, err := runner.PublicSchemaEmpty(ctx)
	if err != nil {
		return err
	}
	if !empty {
		return fmt.Errorf("public schema is not empty after down")
	}
	if err := verifyUp(ctx, runner, db, probes, repoRoot, testDSN); err != nil {
		return fmt.Errorf("up-again: %w", err)
	}
	if err := cleanupAuditRetentionTestRole(ctx, db); err != nil {
		return fmt.Errorf("clean audit retention test role after replay: %w", err)
	}
	if err := runner.DownAll(ctx); err != nil {
		return err
	}
	fmt.Printf("PostgreSQL 16 migration matrix passed for %s\n", databaseName)
	return nil
}

// cleanupAuditRetentionTestRole removes global-role state created by the
// PostgreSQL contract suite. Roles are cluster-scoped rather than database-
// scoped, so dropping the disposable database alone cannot clean a temporary
// principal's membership in audit_retention_purger. This belongs to the test
// harness, not to the production migration down path.
func cleanupAuditRetentionTestRole(ctx context.Context, db *sql.DB) error {
	const role = "audit_retention_purger"
	rows, err := db.QueryContext(ctx, `SELECT member_role.rolname
FROM pg_auth_members membership
JOIN pg_roles granted_role ON granted_role.oid = membership.roleid
JOIN pg_roles member_role ON member_role.oid = membership.member
WHERE granted_role.rolname = $1`, role)
	if err != nil {
		return err
	}
	defer rows.Close()
	var members []string
	for rows.Next() {
		var member string
		if err := rows.Scan(&member); err != nil {
			return err
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, member := range members {
		if _, err := db.ExecContext(ctx, "REVOKE "+quoteRoleName(role)+" FROM "+quoteRoleName(member)); err != nil {
			return err
		}
	}
	// Revokes grants made to the migration-created role, which PostgreSQL also
	// counts as dependencies when the role is removed.
	if _, err := db.ExecContext(ctx, "DROP OWNED BY "+quoteRoleName(role)); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoteRoleName(role))
	return err
}

func quoteRoleName(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func verifyLegacyBackendUpgrade(ctx context.Context, runner *migrations.Runner, db *sql.DB) error {
	if err := runner.UpTo(ctx, "000005"); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAZ','legacy-upgrade','Legacy Upgrade')`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO config_snapshot(tenant_id,config_version,schema_version,payload,content_digest,state,actor_id,reason_code,correlation_id,trace_id,published_at) VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAZ',1,1,'{}',repeat('a',64),'published','migration','upgrade','upgrade','upgrade',now())`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO backend_binding(tenant_id,config_version,domain,backend_type,backend_ref,credential_ref,credential_version,capabilities) VALUES('t_01ARZ3NDEKTSV4RRFFQ69G5FAZ',1,'session','postgres','postgres://legacy','secret://legacy',3,ARRAY['atomic_turn_commit'])`); err != nil {
		return err
	}
	if err := runner.UpTo(ctx, "000006"); err != nil {
		return err
	}
	if err := runner.Ready(ctx); err == nil {
		return fmt.Errorf("partial migration set reported ready")
	}
	var status, provider, backendRef string
	var version int64
	err := db.QueryRowContext(ctx, `SELECT p.status,v.provider,v.configuration->>'backend_ref',b.backend_version FROM backend_binding b JOIN backend_profile p USING(tenant_id,backend_profile_id) JOIN backend_profile_revision v USING(tenant_id,backend_profile_id) WHERE b.tenant_id='t_01ARZ3NDEKTSV4RRFFQ69G5FAZ' AND b.config_version=1 AND b.domain='session'`).Scan(&status, &provider, &backendRef, &version)
	if err != nil {
		return err
	}
	if status != "suspended" || provider != "postgres" || backendRef != "postgres://legacy" || version != 1 {
		return fmt.Errorf("unexpected migrated binding status=%s provider=%s ref=%s version=%d", status, provider, backendRef, version)
	}
	return nil
}

func verifyUp(ctx context.Context, runner *migrations.Runner, db *sql.DB, probes, repoRoot, dsn string) error {
	if err := runner.Up(ctx); err != nil {
		return err
	}
	if err := runner.Up(ctx); err != nil {
		return fmt.Errorf("repeated up: %w", err)
	}
	if err := runner.Ready(ctx); err != nil {
		return fmt.Errorf("migration readiness: %w", err)
	}
	if _, err := db.ExecContext(ctx, probes); err != nil {
		return fmt.Errorf("contract probes: %w", err)
	}
	// These PostgreSQL contract packages intentionally use a shared disposable
	// database and some fixtures truncate cross-domain tables. Run them
	// serially; the default package parallelism otherwise creates test-order
	// races that look like random foreign-key/version conflicts.
	command := exec.CommandContext(ctx, "go", "test", "-p=1", "-count=1", "./trpcservice/agentapp/postgres", "./trpcservice/audit/postgres", "./trpcservice/audit/purgebusiness/postgres", "./trpcservice/config/postgres", "./trpcservice/governance/postgres", "./trpcservice/migration/knowledgedriver/postgres", "./trpcservice/provider/postgres", "./trpcservice/skill/postgres", "./trpcservice/storage/artifact/postgres", "./trpcservice/storage/knowledge/postgres", "./trpcservice/storage/messaging/postgres", "./trpcservice/storage/session/postgres", "./trpcservice/storage/summary/postgres", "./trpcservice/tenant/postgres")
	command.Dir = repoRoot
	command.Env = append(os.Environ(), "TRPC_MIGRATION_TEST=1", "TRPC_POSTGRES_TEST_DSN="+dsn)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("PostgreSQL repository contracts: %w\n%s", err, output)
	}
	if os.Getenv("TRPC_RUNTIME_TEST") == "1" {
		command = exec.CommandContext(ctx, "go", "test", "-count=1", "./trpcservice/integration")
		command.Dir = repoRoot
		command.Env = append(os.Environ(), "TRPC_RUNTIME_TEST=1", "TRPC_POSTGRES_TEST_DSN="+dsn)
		output, err = command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("runtime slice: %w\n%s", err, output)
		}
	}
	return nil
}

func testDSNForDatabase(adminDSN, databaseName string) (string, error) {
	trimmed := strings.TrimSpace(adminDSN)
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		parsed.Path = "/" + databaseName
		return parsed.String(), nil
	}
	dsn := trimmed + " dbname=" + quoteConninfoValue(databaseName)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	if config.Database != databaseName {
		return "", fmt.Errorf("failed to target test database %q", databaseName)
	}
	return dsn, nil
}

func quoteConninfoValue(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

func randomDatabaseName() (string, error) {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	name := "trpc_agent_service_test_" + hex.EncodeToString(bytes)
	if !strings.HasPrefix(name, "trpc_agent_service_test_") {
		return "", fmt.Errorf("unsafe database name")
	}
	return name, nil
}
