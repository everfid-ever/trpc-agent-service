#!/usr/bin/env bash
set -euo pipefail

if [[ "${TRPC_MIGRATION_TEST:-}" != "1" ]]; then
  echo "refusing: set TRPC_MIGRATION_TEST=1 for an explicit disposable test database" >&2
  exit 2
fi
if [[ -z "${TRPC_MIGRATION_TEST_DSN:-}" ]]; then
  echo "refusing: TRPC_MIGRATION_TEST_DSN is required" >&2
  exit 2
fi
if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required" >&2
  exit 2
fi

database_name="$(psql "${TRPC_MIGRATION_TEST_DSN}" -XAtqc 'select current_database()')"
server_major="$(psql "${TRPC_MIGRATION_TEST_DSN}" -XAtqc "select current_setting('server_version_num')::int / 10000")"
if [[ ! "${database_name}" =~ ^trpc_agent_service_test_[a-zA-Z0-9_]+$ ]]; then
  echo "refusing destructive migration test for database: ${database_name}" >&2
  exit 2
fi
if [[ "${server_major}" != "16" ]]; then
  echo "PostgreSQL 16 is required, found major ${server_major}" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

create_metadata() {
  psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -qc \
    'CREATE TABLE IF NOT EXISTS public.schema_migrations(version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())'
}

apply_up() {
  create_metadata
  while IFS= read -r up_file; do
    version="$(basename "${up_file}" | cut -d_ -f1)"
    checksum="$(shasum -a 256 "${up_file}" | awk '{print $1}')"
    applied="$(psql "${TRPC_MIGRATION_TEST_DSN}" -XAtqc "select checksum from public.schema_migrations where version='${version}'")"
    if [[ -n "${applied}" ]]; then
      [[ "${applied}" == "${checksum}" ]] || { echo "checksum mismatch for ${version}" >&2; exit 1; }
      continue
    fi
    psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -f "${up_file}"
    psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -v version="${version}" -v checksum="${checksum}" -qc \
      "INSERT INTO public.schema_migrations(version,checksum) VALUES (:'version',:'checksum')"
  done < <(find "${repo_root}/migrations" -maxdepth 1 -name '*.up.sql' | sort)
}

apply_down() {
  while IFS= read -r down_file; do
    psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -f "${down_file}"
  done < <(find "${repo_root}/migrations" -maxdepth 1 -name '*.down.sql' | sort -r)
  psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -qc 'DROP TABLE IF EXISTS public.schema_migrations'
}

assert_empty() {
  remaining="$(psql "${TRPC_MIGRATION_TEST_DSN}" -XAtqc "select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relkind in ('r','p','v','m','S','f')")"
  functions="$(psql "${TRPC_MIGRATION_TEST_DSN}" -XAtqc "select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public'")"
  [[ "${remaining}" == "0" && "${functions}" == "0" ]] || { echo "public schema is not empty: relations=${remaining}, functions=${functions}" >&2; exit 1; }
}

apply_up
apply_up
psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -f "${repo_root}/scripts/postgres_contract_probes.sql"
apply_down
assert_empty
apply_up
psql "${TRPC_MIGRATION_TEST_DSN}" -X -v ON_ERROR_STOP=1 -f "${repo_root}/scripts/postgres_contract_probes.sql"

echo "PostgreSQL 16 migration matrix passed for ${database_name}"
