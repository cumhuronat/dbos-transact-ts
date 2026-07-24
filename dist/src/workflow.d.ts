import { type SystemDatabase } from './system_database';
import { ConfiguredInstance } from './decorators';
import { type PollingOptions } from './dbos';
import { DuplicationPolicy, EnqueueOptions } from './system_database';
/**
 * Validate that custom workflow attributes, if provided, are a plain key-value object that
 * can be serialized to JSON for storage in the JSONB `attributes` column. A key-value object
 * is required because attributes are queried with the `@>` containment filter; scalars and
 * arrays would store but never match meaningfully. Called at the workflow-creation entry
 * points (`startWorkflow`/enqueue) so invalid input fails fast at the call site rather than
 * surfacing as a database error at insert time.
 */
export declare function validateWorkflowAttributes(attributes: unknown): void;
export interface WorkflowParams {
    workflowUUID?: string;
    configuredInstance?: ConfiguredInstance | null;
    queueName?: string;
    executeWorkflow?: boolean;
    timeoutMS?: number | null;
    deadlineEpochMS?: number;
    enqueueOptions?: EnqueueOptions;
    duplicationPolicy?: DuplicationPolicy;
    workflowAttributes?: Record<string, unknown>;
}
export declare const DEFAULT_MAX_RECOVERY_ATTEMPTS = 100;
export type WorkflowSerializationFormat = undefined | 'native' | 'portable';
/**
 * An object with a `parse` method that validates and optionally transforms input.
 * Compatible with Zod schemas, AJV wrappers, or any custom validator.
 */
export interface InputSchema {
    /** Validate (and optionally transform) workflow input arguments.
     *  Receives the arguments as an array (tuple). Should throw on validation failure.
     *  Return the validated/transformed arguments array. */
    parse(input: unknown): unknown;
}
/**
 * Configuration for `DBOS.workflow` functions
 */
export interface WorkflowConfig {
    /** Maximum number of recovery attempts to make on workflow function, before sending to dead-letter queue */
    maxRecoveryAttempts?: number;
    /** Name to use */
    name?: string;
    /** Default serialization to use */
    serialization?: WorkflowSerializationFormat;
    /** Schema for validating and transforming workflow input arguments.
     *  Must have a `.parse()` method (compatible with Zod, AJV wrappers, etc.).
     *  The schema receives the arguments as an array (tuple) and should return
     *  the validated/transformed array. Runs before the workflow function on
     *  every invocation (direct call, queue dispatch, and recovery). */
    inputSchema?: InputSchema;
}
export interface WorkflowStatus {
    readonly workflowID: string;
    readonly status: string;
    readonly workflowName: string;
    readonly workflowClassName: string;
    readonly workflowConfigName?: string;
    readonly queueName?: string;
    readonly authenticatedUser?: string;
    readonly assumedRole?: string;
    readonly authenticatedRoles?: string[];
    readonly input?: unknown[];
    readonly output?: unknown;
    readonly error?: unknown;
    readonly executorId?: string;
    readonly applicationVersion?: string;
    readonly createdAt: number;
    readonly updatedAt?: number;
    readonly timeoutMS?: number;
    readonly deadlineEpochMS?: number;
    readonly deduplicationID?: string;
    readonly priority: number;
    readonly queuePartitionKey?: string;
    readonly dequeuedAt?: number;
    readonly delayUntilEpochMS?: number;
    readonly completedAt?: number;
    readonly forkedFrom?: string;
    readonly wasForkedFrom?: boolean;
    readonly parentWorkflowID?: string;
    readonly attributes?: Record<string, unknown>;
    readonly scheduleName?: string;
    readonly applicationID: string;
    readonly request?: object;
    readonly recoveryAttempts?: number;
}
export interface GetWorkflowsInput {
    workflowIDs?: string[];
    workflowName?: string | string[];
    status?: WorkflowStatusString | WorkflowStatusString[];
    startTime?: string;
    endTime?: string;
    completedAfter?: string;
    completedBefore?: string;
    dequeuedAfter?: string;
    dequeuedBefore?: string;
    authenticatedUser?: string | string[];
    applicationVersion?: string | string[];
    executorId?: string | string[];
    workflow_id_prefix?: string | string[];
    queueName?: string | string[];
    queuesOnly?: boolean;
    forkedFrom?: string | string[];
    wasForkedFrom?: boolean;
    parentWorkflowID?: string | string[];
    hasParent?: boolean;
    attributes?: Record<string, unknown>;
    scheduleName?: string | string[];
    limit?: number;
    offset?: number;
    sortDesc?: boolean;
    loadInput?: boolean;
    loadOutput?: boolean;
}
export interface GetPendingWorkflowsOutput {
    workflowUUID: string;
    queueName?: string;
}
export interface StepInfo {
    readonly functionID: number;
    readonly name: string;
    readonly output: unknown;
    readonly error: Error | null;
    readonly childWorkflowID: string | null;
    readonly startedAtEpochMs?: number;
    readonly completedAtEpochMs?: number;
}
export interface ListWorkflowStepsOptions {
    limit?: number;
    offset?: number;
}
export interface PgTransactionId {
    txid: string;
}
/** Enumeration of values for workflow status */
export declare const StatusString: {
    /** Workflow has may be running */
    readonly PENDING: "PENDING";
    /** Workflow complete with return value */
    readonly SUCCESS: "SUCCESS";
    /** Workflow complete with error thrown */
    readonly ERROR: "ERROR";
    /** Workflow has exceeded its maximum number of execution or recovery attempts */
    readonly MAX_RECOVERY_ATTEMPTS_EXCEEDED: "MAX_RECOVERY_ATTEMPTS_EXCEEDED";
    /** Workflow is being, or has been, cancelled */
    readonly CANCELLED: "CANCELLED";
    /** Workflow is on a `WorkflowQueue` and has not yet started */
    readonly ENQUEUED: "ENQUEUED";
    /** Workflow is on a `WorkflowQueue` waiting for a delay to expire before it can start */
    readonly DELAYED: "DELAYED";
};
export type WorkflowStatusString = 'PENDING' | 'SUCCESS' | 'ERROR' | 'MAX_RECOVERY_ATTEMPTS_EXCEEDED' | 'CANCELLED' | 'ENQUEUED' | 'DELAYED';
export declare function isWorkflowActive(status: string): boolean;
/**
 * Object representing an active or completed workflow execution, identified by the workflow UUID.
 * Allows retrieval of information about the workflow.
 */
export interface WorkflowHandle<R> {
    /**
     * Retrieve the workflow's status.
     * Statuses are updated asynchronously.
     */
    getStatus(): Promise<WorkflowStatus | null>;
    /**
     * Await workflow completion and return its result.
     */
    getResult(options?: PollingOptions): Promise<R>;
    /**
     * Return the workflow's ID
     */
    get workflowID(): string;
    /**
     * Return the workflow's inputs
     */
    getWorkflowInputs<T extends any[]>(): Promise<T>;
}
export interface InternalWFHandle<R> extends WorkflowHandle<R> {
    getResult(optionsOrFuncIdForGet?: PollingOptions | number): Promise<R>;
}
/**
 * The handle returned when invoking a workflow with DBOSExecutor.workflow
 */
export declare class InvokedHandle<R> implements InternalWFHandle<R> {
    readonly systemDatabase: SystemDatabase;
    readonly workflowPromise: Promise<R>;
    readonly workflowUUID: string;
    readonly workflowName: string;
    constructor(systemDatabase: SystemDatabase, workflowPromise: Promise<R>, workflowUUID: string, workflowName: string);
    getWorkflowUUID(): string;
    get workflowID(): string;
    getStatus(): Promise<WorkflowStatus | null>;
    getResult(optionsOrFuncIdForGet?: PollingOptions | number): Promise<R>;
    getWorkflowInputs<T extends any[]>(): Promise<T>;
}
/**
 * The handle returned when retrieving a workflow with DBOSExecutor.retrieve
 */
export declare class RetrievedHandle<R> implements InternalWFHandle<R> {
    readonly systemDatabase: SystemDatabase;
    readonly workflowUUID: string;
    constructor(systemDatabase: SystemDatabase, workflowUUID: string);
    getWorkflowUUID(): string;
    get workflowID(): string;
    getStatus(): Promise<WorkflowStatus | null>;
    getResult(optionsOrFuncIdForGet?: PollingOptions | number): Promise<R>;
    getWorkflowInputs<T extends any[]>(): Promise<T>;
}
//# sourceMappingURL=workflow.d.ts.map