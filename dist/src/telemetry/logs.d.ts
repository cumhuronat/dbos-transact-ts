import { TelemetryCollector } from './collector';
import { LoggerConfig } from '../dbos-executor';
import { DBOSSpan } from './traces';
/*****************/
/*****************/
export type ContextualMetadata = {
    includeContextMetadata?: boolean;
    span?: DBOSSpan;
};
export interface StackTrace {
    stack?: string;
}
export declare class GlobalLogger {
    private readonly telemetryCollector?;
    private readonly logger;
    readonly addContextMetadata: boolean;
    private isLogging;
    constructor(telemetryCollector?: TelemetryCollector | undefined, config?: LoggerConfig, appName?: string);
    info(logEntry: unknown, metadata?: ContextualMetadata): void;
    debug(logEntry: unknown, metadata?: ContextualMetadata): void;
    warn(logEntry: unknown, metadata?: ContextualMetadata): void;
    error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void;
    destroy(): Promise<void>;
}
/******************/
/******************/
/**
 * The logger interface used throughout DBOS. Implement this to supply a custom
 * logger through `DBOSConfig.logger` (for example, an adapter over an existing
 * logging service).
 *
 * Contract for custom implementations:
 * - Log entries arrive as strings: DBOS stringifies non-string entries before
 *   delegating, and `error()` receives the message of an `Error` with its
 *   stack trace (including any `cause` chain) in `metadata.stack`.
 * - When called from a workflow or step, `metadata.span?.attributes` carries
 *   the operation context (workflow ID, operation name and type, etc.).
 * - DBOS does not filter by `logLevel` before delegating; level routing is the
 *   implementation's responsibility.
 * - DBOS never flushes or closes the logger; the caller owns its lifecycle.
 * - Implementations must not log back through `DBOS.logger`, which could
 *   recurse.
 */
export interface DLogger {
    info(logEntry: unknown, metadata?: ContextualMetadata): void;
    debug(logEntry: unknown, metadata?: ContextualMetadata): void;
    warn(logEntry: unknown, metadata?: ContextualMetadata): void;
    error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void;
}
export declare class DBOSContextualLogger implements DLogger {
    private readonly globalLogger;
    readonly ctx: () => DBOSSpan | undefined;
    readonly includeContextMetadata: boolean;
    constructor(globalLogger: GlobalLogger, ctx: () => DBOSSpan | undefined);
    info(logEntry: unknown, metadata?: ContextualMetadata): void;
    debug(logEntry: unknown, metadata?: ContextualMetadata): void;
    warn(logEntry: unknown, metadata?: ContextualMetadata): void;
    error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void;
}
export declare class DBOSConsoleLogger implements DLogger {
    readonly config: LoggerConfig;
    private readonly levelValue;
    constructor(config: LoggerConfig);
    info(logEntry: unknown, _metadata?: ContextualMetadata): void;
    debug(logEntry: unknown, _metadata?: ContextualMetadata): void;
    warn(logEntry: unknown, _metadata?: ContextualMetadata): void;
    error(inputError: unknown, metadata?: ContextualMetadata & StackTrace): void;
}
//# sourceMappingURL=logs.d.ts.map