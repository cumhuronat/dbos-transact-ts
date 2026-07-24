import type { ClientBase } from 'pg';
export type DBMigration = {
    name?: string;
    pg?: ReadonlyArray<string>;
    sqlite3?: ReadonlyArray<string>;
    /**
     * If true, the migration is executed without wrapping it in any helper that
     * suppresses errors, and its statements must be safe to run outside a
     * transaction (e.g. `CREATE INDEX CONCURRENTLY`). The runner also cleans up
     * indexes left INVALID by a previously interrupted CONCURRENTLY build before
     * running the migration.
     */
    online?: boolean;
};
/** Get the current DB version, or 0 if table is missing/empty. */
export declare function getCurrentSysDBVersion(client: ClientBase, schemaName?: string): Promise<number>;
export type PgMigratorOptions = {
    ignoreErrorCodes?: ReadonlySet<string>;
    onWarn?: (msg: string, err?: unknown) => void;
    isCockroach?: boolean;
};
/**
 * Apply all migrations greater than the current DB version.
 * - Reads current version (0 if table missing/empty)
 * - Applies migrations in order
 * - After each migration, persists `dbos_migrations.version` so partial
 *   progress is recorded
 * - Warns if current version > max known (likely newer software concurrently)
 */
export declare function runSysMigrationsPg(client: ClientBase, allMigrations: ReadonlyArray<DBMigration>, schemaName?: string, opts?: PgMigratorOptions): Promise<{
    fromVersion: number;
    toVersion: number;
    appliedCount: number;
    skippedCount: number;
    notice?: string;
}>;
//# sourceMappingURL=migration_runner.d.ts.map