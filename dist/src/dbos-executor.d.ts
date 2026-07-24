import { type WorkflowHandle, type WorkflowParams, type WorkflowStatus, type StepInfo, type ListWorkflowStepsOptions } from './workflow';
import { type StepConfig } from './step';
import { TelemetryCollector } from './telemetry/collector';
import { Tracer } from './telemetry/traces';
import { DBOSContextualLogger, DLogger, GlobalLogger } from './telemetry/logs';
import { SystemDatabase, type WorkflowStatusInternal, type SystemDatabaseStoredResult } from './system_database';
import { TypedAsyncFunction } from './decorators';
import { type step_info } from '../schemas/system_db_schema';
import { DBOSSerializer } from './serialization';
import { GetWorkflowsInput } from '.';
import { WorkflowQueue } from './wfqueue';
import { Pool } from 'pg';
export declare const DBOS_QUEUE_MIN_PRIORITY = 1;
export declare const DBOS_QUEUE_MAX_PRIORITY: number;
export interface DBOSConfig {
    name?: string;
    systemDatabaseUrl?: string;
    systemDatabasePoolSize?: number;
    systemDatabasePool?: Pool;
    systemDatabaseSchemaName?: string;
    /**
     * Maximum number of DB-backed polling reads (from wait operations such as
     * `getResult`, `recv`, and `getEvent`) that may run concurrently against the
     * system database pool. This keeps high-fan-out polling from checking out
     * every pool client and starving control-plane operations such as
     * enqueue/dequeue, status writes, recovery, and cancellation.
     *
     * Defaults to half the system database pool size (minimum 1). Set to a
     * non-positive value to disable the limiter.
     */
    systemDatabasePollingConcurrency?: number;
    enableOTLP?: boolean;
    tracingEnabled?: boolean;
    logLevel?: string;
    /**
     * A custom logger to which DBOS directs all its internal logging, replacing
     * the built-in console and OTLP log sinks. See {@link DLogger} for the
     * contract implementations must follow. When set:
     * - `logLevel` does not filter calls to it; level routing is its job.
     * - Logs are not exported over OTLP even if `enableOTLP` is on (traces are
     *   unaffected).
     * - DBOS never flushes or closes it; the caller owns its lifecycle.
     */
    logger?: DLogger;
    addContextMetadata?: boolean;
    otlpTracesEndpoints?: string[];
    otlpLogsEndpoints?: string[];
    /**
     * How DBOS span attribute names are emitted to OTLP. Defaults to `'legacy'`
     * for backward compatibility. Set to `'semconv'` to emit OTel-style names
     * under the `dbos.*` namespace. See {@link OtelAttributeFormat}.
     */
    otelAttributeFormat?: OtelAttributeFormat;
    adminPort?: number;
    runAdminServer?: boolean;
    applicationVersion?: string;
    executorID?: string;
    serializer?: DBOSSerializer;
    enablePatching?: boolean;
    /**
     * Restrict this process to only dequeue from the listed queues. Each entry
     * is either a `WorkflowQueue` instance or the name of a queue (in-memory
     * or database-backed). Names that match nothing at launch are deferred —
     * a database-backed queue registered later under that name will be picked
     * up by the supervisor.
     */
    listenQueues?: (WorkflowQueue | string)[];
    /**
     * Maximum number of independent queue dispatch cycles that may run concurrently
     * in this executor. Defaults to 3. Set to 1 to serialize queue dispatch.
     * This does not limit workflow concurrency or system-database polling reads.
     *
     * Each concurrent dispatch checks out a system-database connection for the
     * duration of its dequeue transaction, so values approaching
     * `systemDatabasePoolSize` (default 10, of which the polling limiter already
     * reserves half) leave little of the pool for control-plane operations.
     */
    maxConcurrentQueueDispatches?: number;
    schedulerPollingIntervalMs?: number;
    useListenNotify?: boolean;
    /** Interval (ms) for coalescing LISTEN/NOTIFY notifications (streams and events) off the write path; bounds read latency. Default 10, min 1. */
    notificationCoalesceMs?: number;
    /**
     * Enable the hint-only wake NOTIFYs for queue enqueue and workflow completion (default true).
     * These are emitted app-side ONLY at ENQUEUED/terminal transitions and coalesced, so they carry
     * no per-update cost on the workflow_status table; the poll loop is the unchanged correctness
     * floor. Set false to keep streams/events/recv NOTIFY on while paying nothing for wakes — for the
     * most write-heavy workloads (workflow_status seen at >40K updates/s). Also requires
     * useListenNotify; it is a no-op wherever LISTEN/NOTIFY is unavailable.
     */
    enableWakeNotifications?: boolean;
}
export interface DBOSRuntimeConfig {
    admin_port: number;
    runAdminServer: boolean;
    start: string[];
    setup: string[];
}
export interface TelemetryConfig {
    logs?: LoggerConfig;
    OTLPExporter?: OTLPExporterConfig;
    otelAttributeFormat?: OtelAttributeFormat;
}
/**
 * How DBOS span attribute names are emitted to OTLP.
 *
 * - `'legacy'` (default) — original DBOS names (e.g. `operationUUID`, `applicationID`).
 *   Preserves backward compatibility with existing dashboards and the Python
 *   `dbos-transact` SDK.
 * - `'semconv'` — OpenTelemetry-style names under the `dbos.*` namespace (e.g.
 *   `dbos.operation.workflow_id`, `dbos.application.id`). Follows
 *   https://opentelemetry.io/docs/specs/semconv/general/attribute-naming/.
 */
export type OtelAttributeFormat = 'legacy' | 'semconv';
export interface OTLPExporterConfig {
    logsEndpoint?: string[];
    tracesEndpoint?: string[];
}
export interface LoggerConfig {
    logLevel?: string;
    silent?: boolean;
    addContextMetadata?: boolean;
    forceConsole?: boolean;
    logger?: DLogger;
}
export type DBOSConfigInternal = {
    name?: string;
    systemDatabaseUrl: string;
    sysDbPoolSize?: number;
    systemDatabasePollingConcurrency?: number;
    systemDatabasePool?: Pool;
    systemDatabaseSchemaName: string;
    serializer: DBOSSerializer;
    telemetry: TelemetryConfig;
    schedulerPollingIntervalMs?: number;
    maxConcurrentQueueDispatches?: number;
    useListenNotify: boolean;
    notificationCoalesceMs?: number;
    enableWakeNotifications: boolean;
    http?: {
        cors_middleware?: boolean;
        credentials?: boolean;
        allowed_origins?: string[];
    };
};
export interface InternalWorkflowParams extends WorkflowParams {
    readonly tempWfType?: string;
    readonly tempWfName?: string;
    readonly tempWfClass?: string;
    readonly isRecoveryDispatch?: boolean;
    readonly isQueueDispatch?: boolean;
}
/** Options for assembling an ENQUEUED workflow row without persisting it. */
export interface PrepareEnqueuedWorkflowOptions {
    /** Queue the workflow is enqueued on. */
    readonly queueName: string;
    /** Workflow ID. Doubles as the idempotency key for a batch insert, so it must be deterministic. */
    readonly workflowID: string;
    /**
     * Partition key for a partitioned queue. Not validated here: a partitioned queue only ever
     * dequeues rows that carry a key, so a row enqueued onto one without a key sits ENQUEUED
     * forever rather than failing.
     */
    readonly queuePartitionKey?: string;
}
export declare const OperationType: {
    readonly HANDLER: "handler";
    readonly WORKFLOW: "workflow";
    readonly TRANSACTION: "transaction";
    readonly STEP: "step";
};
export declare const TempWorkflowType: {
    readonly step: "step";
    readonly send: "send";
};
/**
 * State item to be kept in the DBOS system database on behalf of clients
 */
export interface DBOSExternalState {
    /** Name of event receiver service */
    service: string;
    /** Fully qualified function name for which state is kept */
    workflowFnName: string;
    /** subkey within the service+workflowFnName */
    key: string;
    /** Value kept for the service+workflowFnName+key combination */
    value?: string;
    /** Updated time (used to version the value) */
    updateTime?: number;
    /** Updated sequence number (used to version the value) */
    updateSeq?: bigint;
}
export interface DBOSExecutorOptions {
    systemDatabase?: SystemDatabase;
}
export declare class DBOSExecutor {
    #private;
    readonly config: DBOSConfigInternal;
    initialized: boolean;
    readonly systemDatabase: SystemDatabase;
    readonly telemetryCollector: TelemetryCollector;
    static readonly defaultNotificationTimeoutSec = 60;
    readonly systemDBSchemaName: string;
    readonly logger: GlobalLogger;
    readonly ctxLogger: DBOSContextualLogger;
    readonly tracer: Tracer;
    readonly serializer: DBOSSerializer;
    readonly executorID: string;
    static globalInstance: DBOSExecutor | undefined;
    constructor(config: DBOSConfigInternal, { systemDatabase }?: DBOSExecutorOptions);
    get appName(): string | undefined;
    init(): Promise<void>;
    destroy(): Promise<void>;
    static reviveResultOrError<R = unknown>(r: SystemDatabaseStoredResult, serializer: DBOSSerializer): Promise<R>;
    workflow<T extends unknown[], R>(wf: TypedAsyncFunction<T, R>, params: InternalWorkflowParams, ...args: T): Promise<WorkflowHandle<R>>;
    /**
     * Build, without persisting, an ENQUEUED status row for `wf` on `options.queueName`.
     *
     * For batch enqueuers (e.g. a Kafka consumer) that persist many rows in one transaction via
     * {@link SystemDatabase.enqueueWorkflows}. Any ambient DBOS context is ignored: the workflow ID and
     * enqueue options are passed explicitly, and the row inherits no parent, auth, or attributes.
     */
    prepareEnqueuedWorkflow<T extends unknown[], R>(wf: TypedAsyncFunction<T, R>, args: T, options: PrepareEnqueuedWorkflowOptions): Promise<WorkflowStatusInternal>;
    internalWorkflow<T extends unknown[], R>(wf: TypedAsyncFunction<T, R>, params: InternalWorkflowParams, callerID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>>;
    /**
     * Look up an in-memory workflow queue by name. Returns `undefined` for
     * names that are not registered in-process; database-backed queues are
     * not in this map, so callers using this for sync validation should treat
     * `undefined` as "no in-process information" rather than as an error.
     */
    getQueueByName(name: string): WorkflowQueue | undefined;
    runStepTempWF<T extends unknown[], R>(stepFn: TypedAsyncFunction<T, R>, params: WorkflowParams, ...args: T): Promise<R>;
    startStepTempWF<T extends unknown[], R>(stepFn: TypedAsyncFunction<T, R>, params: InternalWorkflowParams, callerWFID?: string, callerFunctionID?: number, ...args: T): Promise<WorkflowHandle<R>>;
    /**
     * Execute a step function.
     * If it encounters any error, retry according to its configured retry policy until the maximum number of attempts is reached, then throw an DBOSError.
     * The step may execute many times, but once it is complete, it will not re-execute.
     */
    callStepFunction<T extends unknown[], R>(stepFn: TypedAsyncFunction<T, R>, stepFnName: string | undefined, stepConfig: StepConfig | undefined, clsInst: object | null, ...args: T): Promise<R>;
    /**
     * Wait for a workflow to emit an event, then return its value.
     */
    getEvent<T>(workflowUUID: string, key: string, timeoutSeconds?: number, pollingIntervalMs?: number): Promise<T | null>;
    /**
     * Fork a workflow.
     * The forked workflow will be assigned a new ID.
     */
    forkWorkflow(workflowID: string, startStep: number, options?: {
        newWorkflowID?: string;
        applicationVersion?: string;
        timeoutMS?: number;
        queueName?: string;
        queuePartitionKey?: string;
        replacementChildren?: Record<string, string>;
    }): Promise<string>;
    /**
     * Retrieve a handle for a workflow UUID.
     */
    retrieveWorkflow<R>(workflowID: string): WorkflowHandle<R>;
    runInternalStep<T>(callback: () => Promise<T>, functionName: string, workflowID: string, functionID: number, childWfId?: string): Promise<T>;
    getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatus | null>;
    listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
    listQueuedWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
    listWorkflowSteps(workflowID: string, loadOutput?: boolean, options?: ListWorkflowStepsOptions): Promise<StepInfo[] | undefined>;
    /**
     * A recovery process that by default runs during executor init time.
     * It runs to completion all pending workflows that were executing when the previous executor failed.
     */
    recoverPendingWorkflows(executorIDs?: string[]): Promise<WorkflowHandle<unknown>[]>;
    initEventReceivers(listenQueues: (WorkflowQueue | string)[] | null): Promise<void>;
    deactivateEventReceivers(stopQueueThread?: boolean): Promise<void>;
    executeWorkflowId(workflowID: string, options?: {
        startNewWorkflow?: boolean;
        isRecoveryDispatch?: boolean;
        isQueueDispatch?: boolean;
    }): Promise<WorkflowHandle<unknown>>;
    getEventDispatchState(svc: string, wfn: string, key: string): Promise<DBOSExternalState | undefined>;
    upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState>;
    getWorkflowSteps(workflowID: string): Promise<step_info[]>;
    /**
      An application's version is computed from a hash of the source of its workflows.
      This is guaranteed to be stable given identical source code because it uses an MD5 hash
      and because it iterates through the workflows in sorted order.
      This way, if the app's workflows are updated (which would break recovery), its version changes.
      App version can be manually set through the DBOS__APPVERSION environment variable.
     */
    computeAppVersion(): string;
    static internalQueue: WorkflowQueue | undefined;
    static createInternalQueue(): void;
}
//# sourceMappingURL=dbos-executor.d.ts.map