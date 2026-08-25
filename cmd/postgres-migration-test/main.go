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
	if err := verifyUp(ctx, runner, db, probes, repoRoot, testDSN); err != nil {
		return err
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
	if err := runner.DownAll(ctx); err != nil {
		return err
	}
	fmt.Printf("PostgreSQL 16 migration matrix passed for %s\n", databaseName)
	return nil
}

func verifyUp(ctx context.Context, runner *migrations.Runner, db *sql.DB, probes, repoRoot, dsn string) error {
	if err := runner.Up(ctx); err != nil {
		return err
	}
	if err := runner.Up(ctx); err != nil {
		return fmt.Errorf("repeated up: %w", err)
	}
	if _, err := db.ExecContext(ctx, probes); err != nil {
		return fmt.Errorf("contract probes: %w", err)
	}
	command := exec.CommandContext(ctx, "go", "test", "-count=1", "./trpcservice/agentapp/postgres", "./trpcservice/config/postgres", "./trpcservice/storage/session/postgres", "./trpcservice/tenant/postgres")
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
