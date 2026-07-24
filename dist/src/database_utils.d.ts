import { Client } from 'pg';
/**
 * The logical thing to provide is the name of the DB to drop (`dbToDrop`) and a connection string with permission (`adminUrl`)
 * However, you can specify `urlToDrop` and we will do our best to find a way to connect and drop it.
 */
export interface DropDatabaseOptions {
    /** Name of the database to drop */
    dbToDrop?: string;
    /** URL of the database to drop */
    urlToDrop?: string;
    /** Admin/alternate DB URL on the same server. If omitted, we'll try `<urlToDrop but with /postgres>` */
    adminUrl?: string;
    /** Optional logger (default: console.log) */
    logger?: (msg: string) => void;
    /** Also try /template1 if /postgres fails (default: true) */
    tryTemplate1Fallback?: boolean;
}
/**
 * Result of a `dropPostgresDatabase` call.
 * Status of `dropped` or `did_not_exist` is a "success",
 *  from the perspective that we are completely sure the DB is not there at the end.
 * Status of `failed` means that the DB still exists,  or we cannot say.
 *  This means "failure" in the sense that the postcondition of a nonexistent DB is not verified.
 */
export type DropDatabaseResult = {
    status: 'dropped' | 'did_not_exist';
    notes: string[];
    message: string;
} | {
    status: 'failed';
    notes: string[];
    hint?: string;
    message: string;
};
/**
 * Drop a postgres database from a postgres server.  This requires a target DB name,
 *  and a way to connect to its server with privileges to issue the drop.  See `opts`.
 * Environment variables are not currently considered.
 *
 * @param opts - Options for connecting to DB and issuing the drop
 * @returns `DropDatabaseResult` indicating success, failures, and any notes or hints
 */
export declare function dropPGDatabase(opts?: DropDatabaseOptions): Promise<DropDatabaseResult>;
/**
 * Result of a `ensurePGDatabase` call.
 * Status of `created` or `already_exists` is a "success",
 *  from the perspective that we are completely sure the DB is there at the end.
 * Status of `failed` means that the DB still doesn't exist, or we cannot say.
 *  This is a "failure" from the sense that the postcondition of an existing DB is not verified.
 */
export type EnsureDatabaseResult = {
    status: 'created' | 'already_exists';
    message: string;
    notes: string[];
} | {
    status: 'failed';
    notes: string[];
    message: string;
    hint?: string;
};
/**
 * The logical thing to provide is the name of the DB to ensure (`dbToEnsure`) and a connection string with permission (`adminUrl`)
 * However, you can specify `urlToEnsure` and we will do our best to find a way to connect and ensure it.
 */
export interface EnsureDatabaseOptions {
    /** Name of the database to ensure */
    dbToEnsure?: string;
    /** URL of the database to ensure */
    urlToEnsure?: string;
    /** Admin/alternate DB URL on the same server. If omitted, we'll try `<urlToEnsure but with /postgres>` */
    adminUrl?: string;
    /** Optional logger (default: console.log) */
    logger?: (msg: string) => void;
    /** Also try /template1 if /postgres fails (default: true) */
    tryTemplate1Fallback?: boolean;
}
export declare function ensurePGDatabase(opts: EnsureDatabaseOptions): Promise<EnsureDatabaseResult>;
export declare function deriveDatabaseUrl(urlStr: string, otherDbName: string): string;
export declare function getPGClientConfig(databaseUrl: string | URL): {
    connectionString: string;
    connectionTimeoutMillis: number;
};
export declare function getDatabaseNameFromUrl(urlStr: string): string;
export declare function maskDatabaseUrl(urlStr: string): string;
export declare function connectToPGDatabase(url: string, log: (m: string) => void): Promise<Client | null>;
export declare function connectToPGAndReportOutcome(url: string, log: (m: string) => void, label: string): Promise<{
    result: 'ok';
    client: Client;
} | {
    result: 'error';
    code?: string;
    message: string;
}>;
export declare function currentDBUserIdentity(client: Client): Promise<{
    user: string;
    isSuperuser: boolean;
}>;
export declare function getPGDatabaseOwner(admin: Client, dbName: string): Promise<string | null>;
//# sourceMappingURL=database_utils.d.ts.map