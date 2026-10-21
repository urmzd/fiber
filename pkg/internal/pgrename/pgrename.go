// Package pgrename moves a database created before fiber's rename forward:
// a store's tables that still carry the old prefix in the current schema,
// with their indexes and constraints, are renamed to the fiber_ prefix. Each
// store's Migrate runs SQL for its own tables in the same transaction, under
// the same advisory lock, before its CREATE IF NOT EXISTS statements, so an
// upgraded database keeps its rows, a fresh one is untouched, and the order
// stores migrate in does not matter.
//
// This is the only place the old name is kept, and only so that an existing
// database can be found and renamed.
package pgrename

import "strings"

// LegacyPrefix is the prefix fiber's Postgres objects had before the rename.
const LegacyPrefix = "mandatum_"

// LegacyAgentSpecDomain is the domain agent-spec hashes were computed under
// before the rename. The registry's migration re-addresses revisions stored
// under it (see agentspec.AgentSpec.HashIn).
const LegacyAgentSpecDomain = "mandatum/agentspec/v1"

// SQL renames each of tables (named without a prefix, "agents") from
// LegacyPrefix to fiber_ in the current schema, with every index and
// constraint on it named with LegacyPrefix, when the old table exists and the
// new name is free. It is idempotent. Table names are fiber's own constants,
// never input.
func SQL(tables ...string) string {
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = "'" + t + "'"
	}
	return `
DO $pgrename$
DECLARE
	t text;
	r record;
	old_prefix constant text := '` + LegacyPrefix + `';
	n constant int := length('` + LegacyPrefix + `');
	tbl regclass;
BEGIN
	FOREACH t IN ARRAY ARRAY[` + strings.Join(quoted, ", ") + `]::text[] LOOP
		tbl := to_regclass(quote_ident(current_schema()) || '.' || quote_ident(old_prefix || t));
		CONTINUE WHEN tbl IS NULL
			OR to_regclass(quote_ident(current_schema()) || '.' || quote_ident('fiber_' || t)) IS NOT NULL;
		FOR r IN SELECT ic.relname FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
			WHERE i.indrelid = tbl AND left(ic.relname, n) = old_prefix
		LOOP
			EXECUTE format('ALTER INDEX %I RENAME TO %I', r.relname, 'fiber_' || substr(r.relname, n + 1));
		END LOOP;
		FOR r IN SELECT con.conname FROM pg_constraint con
			WHERE con.conrelid = tbl AND left(con.conname, n) = old_prefix
		LOOP
			EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I', tbl, r.conname, 'fiber_' || substr(r.conname, n + 1));
		END LOOP;
		EXECUTE format('ALTER TABLE %s RENAME TO %I', tbl, 'fiber_' || t);
	END LOOP;
END
$pgrename$;
`
}
