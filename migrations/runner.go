package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

type Runner struct{ db *sql.DB }

func NewRunner(db *sql.DB) *Runner { return &Runner{db: db} }

func (r *Runner) Up(ctx context.Context) error {
	if _, err := r.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS public.schema_migrations(
version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	all, err := All()
	if err != nil {
		return err
	}
	for _, migration := range all {
		checksum := migrationChecksum(migration.Up)
		var applied string
		err := r.db.QueryRowContext(ctx, `SELECT checksum FROM public.schema_migrations WHERE version=$1`, migration.Version).Scan(&applied)
		if err == nil {
			if applied != checksum {
				return fmt.Errorf("migration %s checksum mismatch", migration.Version)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}
		body, err := transactionBody(migration.Up)
		if err != nil {
			return fmt.Errorf("migration %s: %w", migration.Version, err)
		}
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, body); err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO public.schema_migrations(version,checksum) VALUES($1,$2)`, migration.Version, checksum)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", migration.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) DownAll(ctx context.Context) error {
	all, err := All()
	if err != nil {
		return err
	}
	for index := len(all) - 1; index >= 0; index-- {
		migration := all[index]
		var exists bool
		if err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version=$1)`, migration.Version).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		body, err := transactionBody(migration.Down)
		if err != nil {
			return fmt.Errorf("migration %s: %w", migration.Version, err)
		}
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, body); err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM public.schema_migrations WHERE version=$1`, migration.Version)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("down migration %s: %w", migration.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	_, err = r.db.ExecContext(ctx, `DROP TABLE IF EXISTS public.schema_migrations`)
	return err
}

func (r *Runner) PublicSchemaEmpty(ctx context.Context) (bool, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT
  (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f'))
 + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public')`).Scan(&count)
	return count == 0, err
}

func transactionBody(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(trimmed, "BEGIN;") || !strings.HasSuffix(trimmed, "COMMIT;") {
		return "", fmt.Errorf("migration is not transaction wrapped")
	}
	trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "BEGIN;"))
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, "COMMIT;"))
	return trimmed, nil
}

func migrationChecksum(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}
