import type { DBMigration } from '../migration_runner';
export declare function allMigrations(schemaName?: string, opts?: {
    useListenNotify?: boolean;
    isCockroach?: boolean;
}): ReadonlyArray<DBMigration>;
//# sourceMappingURL=migrations.d.ts.map