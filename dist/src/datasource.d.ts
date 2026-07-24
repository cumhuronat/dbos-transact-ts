import { FunctionName } from './decorators';
/**
 * This interface is to be used for implementers of transactional data sources
 *   This is what gets registered for the transaction control framework
 */
export interface DataSourceTransactionHandler {
    readonly name: string;
    readonly dsType: string;
    /**
     * Will be called by DBOS during launch.
     * This may be a no-op if the DS is initialized before telling DBOS about the DS at all.
     */
    initialize(): Promise<void>;
    /**
     * Will be called by DBOS during attempt at clean shutdown (generally in testing scenarios).
     */
    destroy(): Promise<void>;
    /**
     * Invoke a transaction function
     */
    invokeTransactionFunction<This, Args extends unknown[], Return>(config: unknown, target: This, func: (this: This, ...args: Args) => Promise<Return>, ...args: Args): Promise<Return>;
}
/**
 * This is the suggested interface guideline for presenting to the end user, but not
 *   strictly required.
 */
export interface DBOSDataSource<Config extends {
    name?: string;
}> {
    readonly name: string;
    /**
     * Run the code transactionally within this data source
     *   Implementers should strongly type the config
     * @param callback - Function to run within a transactional context
     * @param name - Step name to show in the system database, traces, etc.
     * @param config - Transaction configuration options
     */
    runTransaction<T>(callback: () => Promise<T>, config?: Config): Promise<T>;
    /**
     * Register function as DBOS transaction, to be called within the context
     *  of a transaction on this data source.
     *
     * Providing a static version of this functionality is optional.
     *
     * @param func - Function to wrap
     * @param config - Transaction settings, including function `name`
     * @param target - Class name, or class ctor/prototype
     * @returns Wrapped function, to be called instead of `func`
     */
    registerTransaction<This, Args extends unknown[], Return>(func: (this: This, ...args: Args) => Promise<Return>, config?: Config & FunctionName): (this: This, ...args: Args) => Promise<Return>;
    /**
     * Produce a Stage 2 method decorator
     * @param config - Configuration to apply to the decorated method
     */
    transaction(config?: Config): <This, Args extends unknown[], Return>(target: object, propertyKey: string, descriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>) => TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>;
}
/**
 * This function is to be called by `DataSourceTransactionHandler` instances,
 *   with bits of user code to be run as transactions.
 * 1. The DS validates the type of config and provides the name
 * 2. The transaction will be started inside here, with a durable sysdb checkpoint.
 * 3. The DS will in turn be called upon to run the callback in a transaction context
 * @param callback - User callback function
 * @param funcName - Function name, for recording in system DB
 * @param options - Data source name and configuration
 * @returns the return from `callback`
 */
export declare function runTransaction<T>(callback: () => Promise<T>, funcName: string, options?: {
    dsName?: string;
    config?: unknown;
}): Promise<T>;
export declare function registerTransaction<This, Args extends unknown[], Return, Config extends FunctionName>(dsName: string, func: (this: This, ...args: Args) => Promise<Return>, config?: Config): (this: This, ...args: Args) => Promise<Return>;
/**
 * Register a transactional data source, that helps DBOS provide
 *  transactional access to user databases
 * @param name - Registered name for the data source
 * @param ds - Transactional data source provider
 */
export declare function registerDataSource(ds: DataSourceTransactionHandler): void;
/** Isolation typically supported by application databases */
export declare const PGIsolationLevel: Readonly<{
    readonly ReadUncommitted: "READ UNCOMMITTED";
    readonly ReadCommitted: "READ COMMITTED";
    readonly RepeatableRead: "REPEATABLE READ";
    readonly Serializable: "SERIALIZABLE";
}>;
type ValuesOf<T> = T[keyof T];
export type PGIsolationLevel = ValuesOf<typeof PGIsolationLevel>;
/**
 * Configuration for Postgres-like transactions
 */
export interface PGTransactionConfig {
    /** Isolation level to request from underlying app database */
    isolationLevel?: PGIsolationLevel;
    /** If set, request read-only transaction from underlying app database */
    readOnly?: boolean;
}
export interface CheckSchemaInstallationReturn {
    schema_exists: number;
    table_exists: number;
}
export declare function checkSchemaInstallationPG(schemaName?: string): string;
export declare function createTransactionCompletionSchemaPG(schemaName?: string): string;
export declare function createTransactionCompletionTablePG(schemaName?: string): string;
export declare function isPGRetriableTransactionError(error: unknown): boolean;
export declare function isPGKeyConflictError(error: unknown): boolean;
export declare function isPGFailedSqlTransactionError(error: unknown): boolean;
export { maskDatabaseUrl, getDatabaseNameFromUrl, deriveDatabaseUrl, getPGClientConfig, connectToPGDatabase, connectToPGAndReportOutcome, DropDatabaseOptions, DropDatabaseResult, dropPGDatabase, EnsureDatabaseOptions, EnsureDatabaseResult, ensurePGDatabase, } from './database_utils';
//# sourceMappingURL=datasource.d.ts.map