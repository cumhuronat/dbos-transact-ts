import { SystemDatabase, type DuplicationPolicy, type VersionInfo, type DebounceParams, type DebounceResult } from './system_database';
import { DLogger } from './telemetry/logs';
import { type GetWorkflowsInput, type StepInfo, type ListWorkflowStepsOptions, type WorkflowHandle, WorkflowSerializationFormat, type WorkflowStatus } from './workflow';
import { type GetEventOptions, type PollingOptions, type WaitFirstOptions, type WaitAllOptions, type SetWorkflowDelayOptions, type CancelWorkflowsOptions } from './dbos';
import { DBOSSerializer } from './serialization';
import { Pool } from 'pg';
import { type WorkflowSchedule, ScheduleOptions } from './scheduler/scheduler';
import { RegisterQueueOptions, WorkflowQueue } from './wfqueue';
/**
 * EnqueueOptions defines the options that can be passed to the `enqueue` method of the DBOSClient.
 * This includes parameters like queue name, workflow name, workflow class name, and other optional settings.
 */
export interface ClientEnqueueOptions {
    /**
     * The name of the queue to which the workflow will be enqueued.
     */
    queueName: string;
    /**
     * The name of the method that will be invoked when the workflow runs.
     */
    workflowName: string;
    /**
     * The name of the class containing the method that will be invoked when the workflow runs.
     * If not provided, an empty string will be used as the class name.
     */
    workflowClassName?: string;
    /**
     * The name of the ConfiguredInstance containing the method that will be invoked when the workflow runs.
     * If not provided, an empty string will be used as the configured instance name.
     */
    workflowConfigName?: string;
    /**
     * An optional identifier for the workflow to ensure idempotency.
     * If not provided, a new UUID will be generated.
     */
    workflowID?: string;
    /**
     * The application version associated with this workflow.
     * If not provided, the version of the DBOS app that first dequeues the workflow will be used.
     */
    appVersion?: string;
    /**
     * Timeout for the workflow execution in milliseconds.
     * Note, timeout starts when the workflow is dequeued.
     * If not provided, the workflow timeout will not be set and the workflow will run to completion.
     */
    workflowTimeoutMS?: number;
    /**
     * An ID used to identify enqueues workflows that will be used for de-duplication.
     * If not provided, no de-duplication will be performed.
     */
    deduplicationID?: string;
    /**
     * Serialization to use for enqueued request
     *   Default is to use the serialization for JS/TS, as this is the most flexible
     *   If `portable_json` is specified, a more limited JSON serialization is used,
     *    allowing cross-language enqueues of workflows with simple semantics
     */
    serializationType?: WorkflowSerializationFormat;
    /**
     * An optional priority for the workflow.
     * Workflows with higher priority will be dequeued first.
     */
    priority?: number;
    /**
     * Partition key for partitioned queues.
     * Required when enqueueing on a partitioned queue.
     */
    queuePartitionKey?: string;
    /**
     * Number of seconds to delay the workflow before it starts executing.
     * The workflow will be in DELAYED status until the delay expires, then transition to ENQUEUED.
     */
    delaySeconds?: number;
    /**
     * How to handle a collision with another workflow that has the same `deduplicationID`
     * on the same queue.
     *   `'reject'` (default): throw `DBOSQueueDuplicatedError`.
     *   `'return-existing'`: return a handle to the existing workflow instead of throwing.
     *     Requires `deduplicationID`. Arguments passed by the colliding caller are discarded
     *     and the handle resolves with the original workflow's result.
     */
    duplicationPolicy?: DuplicationPolicy;
    /**
     * Custom key-value attributes to attach to the workflow at creation.
     * Attributes are searchable via the `attributes` filter of `listWorkflows`.
     */
    attributes?: Record<string, unknown>;
}
/**
 * Options for client send
 */
interface ClientSendOptions {
    /**
     * Serialization to use for sent message
     *   Default is to use the serialization for TS/JS, as this is the most flexible
     *   If `portable_json` is specified, a more limited JSON serialization is used,
     *     allowing cross-language message sends
     */
    serializationType?: WorkflowSerializationFormat;
}
export declare class ClientHandle<R> implements WorkflowHandle<R> {
    readonly systemDatabase: SystemDatabase;
    readonly workflowUUID: string;
    constructor(systemDatabase: SystemDatabase, workflowUUID: string);
    getWorkflowUUID(): string;
    get workflowID(): string;
    getStatus(): Promise<WorkflowStatus | null>;
    getResult(options?: PollingOptions): Promise<R>;
    getWorkflowInputs<T extends unknown[]>(): Promise<T>;
}
/**
 * DBOSClient is the main entry point for interacting with the DBOS system.
 */
export declare class DBOSClient {
    #private;
    readonly serializer: DBOSSerializer;
    private readonly logger;
    private readonly systemDatabase;
    private constructor();
    /**
     * Creates a new instance of the DBOSClient.
     * @param systemDatabaseUrl - The connection string for the system database. This should include the hostname, port, username, password, and database name.
     * @param systemDatabasePool - An optional pre-configured connection pool to use for the system database. If provided, `systemDatabasePoolSize` is ignored.
     * @param serializer - An optional serializer for workflow inputs and outputs. Defaults to JSON serialization.
     * @param systemDatabaseSchemaName - An optional schema name for the system database. Defaults to `dbos`.
     * @param systemDatabasePoolSize - An optional maximum size for the system database connection pool. Defaults to {@link DEFAULT_POOL_SIZE}.
     * @param systemDatabasePollingConcurrency - An optional maximum number of concurrent polling operations. Defaults to half the pool size (minimum 1).
     * @param logger - An optional custom logger to which the client directs all its logging, replacing the built-in console logger.
     * @returns A Promise that resolves with the DBOSClient instance.
     */
    static create({ systemDatabaseUrl, systemDatabasePool, serializer, systemDatabaseSchemaName, systemDatabasePoolSize, systemDatabasePollingConcurrency, logger, }: {
        systemDatabaseUrl: string;
        systemDatabasePool?: Pool;
        serializer?: DBOSSerializer;
        systemDatabaseSchemaName?: string;
        systemDatabasePoolSize?: number;
        systemDatabasePollingConcurrency?: number;
        logger?: DLogger;
    }): Promise<DBOSClient>;
    /**
     * Destroys the underlying database connection.
     * This should be called when the client is no longer needed to clean up resources.
     * @returns A Promise that resolves when database connection is destroyed.
     */
    destroy(): Promise<void>;
    /**
     * Enqueues a workflow for execution.
     * @param options - Options for the enqueue operation, including queue name, workflow name, and other parameters.
     * @param args - Arguments to pass to the workflow upon execution.
     * @returns A Promise that resolves when enqueue is complete, providing a handle to the enqueued workflow.
     */
    enqueue<T extends (...args: any[]) => Promise<any>>(options: ClientEnqueueOptions, ...args: Parameters<T>): Promise<WorkflowHandle<Awaited<ReturnType<T>>>>;
    /**
     * Enqueue a debounced workflow for `DebouncerClient`.
     * The debounce fields are stamped onto the built status here because they are
     * not part of the public enqueue API.
     * @internal
     */
    enqueueDebounced(options: ClientEnqueueOptions, debounceDeadlineEpochMS: number | undefined, args: unknown[]): Promise<string>;
    /**
     * Extend an existing debounced DELAYED workflow, for `DebouncerClient`.
     * @internal
     */
    debounceDelayedWorkflow(params: DebounceParams): Promise<DebounceResult>;
    /**
     * Enqueues a workflow for execution, where the workflow function definition is not
     *   available and may be implemented in another language.
     * @param options - Options for the enqueue operation, including queue name, workflow name, and other parameters.
     * @param positionalArgs - Array of positional arguments to pass to the workflow upon execution.
     * @param namedArgs - Optional object containing named arguments for the target workflow (useful mainly for calling Python functions with kwargs)
     * @returns A Promise that resolves when enqueue is complete, providing a handle to the enqueued workflow.
     */
    enqueuePortable<T = unknown>(options: ClientEnqueueOptions, positionalArgs: unknown[], namedArgs?: {
        [key: string]: unknown;
    }): Promise<WorkflowHandle<T>>;
    /**
     * Register a workflow queue and persist its configuration in the system
     * database. The returned queue's `set` methods write through this
     * client's database.
     *
     * Defaults `onConflict` to `'always_update'` because clients are not
     * associated with an application version.
     */
    registerQueue(name: string, options?: RegisterQueueOptions): Promise<WorkflowQueue>;
    /** Retrieve a database-backed queue by name, or `null` if no row exists. */
    retrieveQueue(name: string): Promise<WorkflowQueue | null>;
    /** Delete a database-backed queue. Pending workflows on it are unrecoverable. */
    deleteQueue(name: string): Promise<void>;
    /**
     * Sends a message to a workflow, identified by destinationID.
     * @param destinationID - The ID of the destination workflow.
     * @param message - The message to send. This can be any serializable object.
     * @param topic - An optional topic to send the message to. If not provided, the default topic will be used.
     * @param idempotencyKey - An optional idempotency key to ensure that the message is only sent once per destination.
     * @returns A Promise that resolves when the message has been sent.
     */
    send<T>(destinationID: string, message: T, topic?: string, idempotencyKey?: string, options?: ClientSendOptions): Promise<void>;
    /**
     * Retrieves an event published by workflowID for a given key.
     * @param workflowID - The ID of the workflow that published the event.
     * @param key - The key associated with the event you want to retrieve.
     * @param options - {@link GetEventOptions} controlling timeout or deadline; if neither is set, times out after 60 seconds
     * @returns A Promise that resolves with the event payload.
     */
    getEvent<T>(workflowID: string, key: string, options?: number | GetEventOptions): Promise<T | null>;
    /**
     * Retrieves a single workflow by its id.
     * @param workflowID - The ID of the workflow to retrieve.
     * @returns a WorkflowHandle that represents the retrieved workflow.
     */
    retrieveWorkflow<T = unknown>(workflowID: string): WorkflowHandle<Awaited<T>>;
    cancelWorkflow(workflowID: string, options?: CancelWorkflowsOptions): Promise<void>;
    cancelWorkflows(workflowIDs: string[], options?: CancelWorkflowsOptions): Promise<void>;
    resumeWorkflow(workflowID: string, options?: {
        queueName?: string;
    }): Promise<void>;
    resumeWorkflows(workflowIDs: string[], options?: {
        queueName?: string;
    }): Promise<void>;
    setWorkflowPriority(workflowID: string, priority: number): Promise<void>;
    setWorkflowDelay(workflowID: string, options: number | SetWorkflowDelayOptions): Promise<void>;
    deleteWorkflow(workflowID: string, deleteChildren?: boolean): Promise<void>;
    deleteWorkflows(workflowIDs: string[], deleteChildren?: boolean): Promise<void>;
    forkWorkflow(workflowID: string, startStep: number, options?: {
        newWorkflowID?: string;
        applicationVersion?: string;
        timeoutMS?: number;
        queueName?: string;
        queuePartitionKey?: string;
        replacementChildren?: Record<string, string>;
    }): Promise<string>;
    getWorkflow(workflowID: string): Promise<WorkflowStatus | undefined>;
    listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
    listQueuedWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatus[]>;
    listWorkflowSteps(workflowID: string, options?: ListWorkflowStepsOptions): Promise<StepInfo[] | undefined>;
    waitFirst(handles: WorkflowHandle<unknown>[], options?: WaitFirstOptions): Promise<WorkflowHandle<unknown>>;
    waitAll<R>(handles: WorkflowHandle<R>[], options?: WaitAllOptions): Promise<WorkflowHandle<R>[]>;
    /**
     * Read values from a stream as an async generator.
     * This function reads values from a stream identified by the workflowID and key,
     * yielding each value in order until the stream is closed or the workflow terminates.
     * @param workflowID - The ID of the workflow that wrote to the stream
     * @param key - The stream key to read from
     * @returns An async generator that yields each value in the stream until the stream is closed
     */
    readStream<T>(workflowID: string, key: string): AsyncGenerator<T, void, unknown>;
    createSchedule(options: {
        scheduleName: string;
        workflowName: string;
        workflowClassName?: string;
        schedule: string;
        context?: unknown;
        options?: ScheduleOptions;
    }): Promise<void>;
    listSchedules(filters?: {
        status?: string | string[];
        workflowName?: string | string[];
        scheduleNamePrefix?: string | string[];
    }): Promise<WorkflowSchedule[]>;
    getSchedule(name: string): Promise<WorkflowSchedule | null>;
    deleteSchedule(name: string): Promise<void>;
    pauseSchedule(name: string): Promise<void>;
    resumeSchedule(name: string): Promise<void>;
    updateSchedule(name: string, updates: {
        schedule?: string;
        context?: unknown;
        automaticBackfill?: boolean;
        cronTimezone?: string | null;
        queueName?: string | null;
    }): Promise<void>;
    applySchedules(schedules: Array<{
        scheduleName: string;
        workflowName: string;
        workflowClassName?: string;
        schedule: string;
        context?: unknown;
        automaticBackfill?: boolean;
        cronTimezone?: string;
        queueName?: string;
    }>): Promise<void>;
    triggerSchedule(name: string): Promise<WorkflowHandle<unknown>>;
    backfillSchedule(name: string, start: Date, end: Date): Promise<WorkflowHandle<unknown>[]>;
    listApplicationVersions(): Promise<VersionInfo[]>;
    getLatestApplicationVersion(): Promise<VersionInfo>;
    setLatestApplicationVersion(versionName: string): Promise<void>;
}
export {};
//# sourceMappingURL=client.d.ts.map