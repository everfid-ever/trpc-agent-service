// Package compliancemigrations owns the deliberately small schema installed
// in the independent compliance PostgreSQL database.
package compliancemigrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"errors"
	"strings"
)

const version = "000001_audit_store"

//go:embed 000001_audit_store.up.sql
var migration string

//go:embed 000001_audit_store.down.sql
var downMigration string

type Runner struct{ DB *sql.DB }

func (r Runner) Up(ctx context.Context) error {
	if r.DB == nil {
		return errors.New("compliance migration database is required")
	}
	if _, err := r.DB.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS compliance`); err != nil {
		return err
	}
	if _, err := r.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS compliance.schema_migrations(
version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	want := checksum(migration)
	var applied string
	err := r.DB.QueryRowContext(ctx, `SELECT checksum FROM compliance.schema_migrations WHERE version=$1`, version).Scan(&applied)
	if err == nil {
		if applied != want {
			return errors.New("compliance migration checksum mismatch")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	body, err := transactionBody(migration)
	if err != nil {
		return err
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The bootstrap schema already exists so the migration remains idempotent
	// only through its immutable version row, not through permissive DDL.
	body = strings.Replace(body, "CREATE SCHEMA compliance;", "", 1)
	if _, err := tx.ExecContext(ctx, body); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO compliance.schema_migrations(version,checksum) VALUES($1,$2)`, version, want); err != nil {
		return err
	}
	return tx.Commit()
}

func (r Runner) Ready(ctx context.Context) error {
	if r.DB == nil {
		return errors.New("compliance migration database is required")
	}
	var applied string
	if err := r.DB.QueryRowContext(ctx, `SELECT checksum FROM compliance.schema_migrations WHERE version=$1`, version).Scan(&applied); err != nil {
		return errors.New("compliance schema is not ready")
	}
	if applied != checksum(migration) {
		return errors.New("compliance migration checksum mismatch")
	}
	return nil
}

func (r Runner) Down(ctx context.Context) error {
	if r.DB == nil {
		return errors.New("compliance migration database is required")
	}
	body, err := transactionBody(downMigration)
	if err != nil {
		return err
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, body); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM compliance.schema_migrations WHERE version=$1`, version); err != nil {
		return err
	}
	return tx.Commit()
}

func transactionBody(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "BEGIN;") || !strings.HasSuffix(value, "COMMIT;") {
		return "", errors.New("compliance migration is not transaction wrapped")
	}
	value = strings.TrimSpace(strings.TrimPrefix(value, "BEGIN;"))
	return strings.TrimSpace(strings.TrimSuffix(value, "COMMIT;")), nil
}

func checksum(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
