/// <reference types="node" />
import { DBOSExternalState } from './dbos-executor';
import { Pool, PoolClient } from 'pg';
import { GetPendingWorkflowsOutput, GetWorkflowsInput, StatusString } from './workflow';
import { operation_outputs, workflow_status, workflow_events, workflow_events_history, streams, SysDBSerializationFormat } from '../schemas/system_db_schema';
import { Semaphore } from './utils';
import { GlobalLogger } from './telemetry/logs';
import { WorkflowQueue } from './wfqueue';
import { DBOSSerializer } from './serialization';
export interface SystemDatabaseStoredResult {
    output?: string | null;
    error?: string | null;
    cancelled?: boolean;
    maxRecoveryAttemptsExceeded?: boolean;
    childWorkflowID?: string | null;
    functionName?: string;
    serialization?: string | null;
}
export interface ExportedWorkflow {
    workflow_status: workflow_status;
    operation_outputs: operation_outputs[];
    workflow_events: workflow_events[];
    workflow_events_history: workflow_events_history[];
    streams: streams[];
}
export declare const DBOS_FUNCNAME_SEND = "DBOS.send";
export declare const DBOS_FUNCNAME_RECV = "DBOS.recv";
export declare const DBOS_FUNCNAME_SETEVENT = "DBOS.setEvent";
export declare const DBOS_FUNCNAME_GETEVENT = "DBOS.getEvent";
export declare const DBOS_FUNCNAME_SLEEP = "DBOS.sleep";
export declare const DBOS_FUNCNAME_GETSTATUS = "getStatus";
export declare const DBOS_FUNCNAME_WRITESTREAM = "DBOS.writeStream";
export declare const DBOS_FUNCNAME_CLOSESTREAM = "DBOS.closeStream";
export declare const DEFAULT_POOL_SIZE = 10;
export declare const DBOS_STREAM_CLOSED_SENTINEL = "__DBOS_STREAM_CLOSED__";
export declare const DBOS_NOTIFICATIONS_CHANNEL = "dbos_notifications_channel";
export declare const DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel";
export declare const DBOS_STREAMS_CHANNEL = "dbos_streams_channel";
export declare const DBOS_QUEUE_WAKEUP_CHANNEL = "dbos_queue_wakeup";
export declare const DBOS_WORKFLOW_COMPLETION_CHANNEL = "dbos_workflow_completion";
export declare const QUEUE_WAKEUP_KEY = "dbos_queue_wakeup";
export declare const DEFAULT_NOTIFICATION_COALESCE_MS = 10;
export interface WorkflowScheduleInternal {
    scheduleId: string;
    scheduleName: string;
    workflowName: string;
    workflowClassName: string;
    schedule: string;
    status: string;
    context: string;
    lastFiredAt: string | null;
    automaticBackfill: boolean;
    cronTimezone: string | null;
    queueName: string | null;
}
export interface WorkflowScheduleUpdate {
    schedule?: string;
    context?: string;
    automaticBackfill?: boolean;
    cronTimezone?: string | null;
    queueName?: string | null;
}
export interface VersionInfo {
    versionId: string;
    versionName: string;
    versionTimestamp: number;
    createdAt: number;
}
export interface QueueRecord {
    name: string;
    concurrency: number | null;
    workerConcurrency: number | null;
    rateLimitMax: number | null;
    rateLimitPeriodSec: number | null;
    priorityEnabled: boolean;
    partitionQueue: boolean;
    pollingIntervalSec: number;
}
/** The subset of a queue record that may be changed after creation. */
export type QueueRecordUpdate = Partial<Omit<QueueRecord, 'name'>>;
export interface WorkflowAggregateRow {
    group: Record<string, string | null>;
    count: number | null;
    minCreatedAt: number | null;
    maxQueueWaitMs: number | null;
    maxTotalLatencyMs: number | null;
}
export interface StepAggregateRow {
    group: Record<string, string | null>;
    count: number | null;
    maxDurationMs: number | null;
}
export interface GetWorkflowAggregatesInput {
    groupByStatus?: boolean;
    groupByName?: boolean;
    groupByQueueName?: boolean;
    groupByExecutorId?: boolean;
    groupByApplicationVersion?: boolean;
    selectCount?: boolean;
    selectMinCreatedAt?: boolean;
    selectMaxQueueWaitMs?: boolean;
    selectMaxTotalLatencyMs?: boolean;
    timeBucketSizeMs?: number;
    status?: string[];
    startTime?: string;
    endTime?: string;
    completedAfter?: string;
    completedBefore?: string;
    dequeuedAfter?: string;
    dequeuedBefore?: string;
    name?: string[];
    appVersion?: string[];
    executorId?: string[];
    queueName?: string[];
    workflowIdPrefix?: string[];
    workflowIDs?: string[];
    authenticatedUser?: string[];
    forkedFrom?: string[];
    wasForkedFrom?: boolean;
    parentWorkflowID?: string[];
    hasParent?: boolean;
    queuesOnly?: boolean;
    attributes?: Record<string, unknown>;
    scheduleName?: string[];
}
export interface GetStepAggregatesInput {
    groupByFunctionName?: boolean;
    groupByStatus?: boolean;
    selectCount?: boolean;
    selectMaxDurationMs?: boolean;
    timeBucketSizeMs?: number;
    status?: string[];
    functionName?: string[];
    workflowIdPrefix?: string[];
    completedAfter?: string;
    completedBefore?: string;
}
export interface WorkflowStatusInternal {
    workflowUUID: string;
    status: string;
    workflowName: string;
    workflowClassName: string;
    workflowConfigName: string;
    queueName?: string;
    authenticatedUser: string;
    output: string | null;
    error: string | null;
    input: string | null;
    assumedRole: string;
    authenticatedRoles: string[];
    request: object;
    executorId: string;
    applicationVersion?: string;
    applicationID: string;
    createdAt: number;
    updatedAt?: number;
    recoveryAttempts?: number;
    timeoutMS?: number;
    deadlineEpochMS?: number;
    deduplicationID?: string;
    priority: number;
    queuePartitionKey?: string;
    startedAtEpochMs?: number;
    forkedFrom?: string;
    wasForkedFrom?: boolean;
    parentWorkflowID?: string;
    serialization: string | null;
    delayUntilEpochMS?: number;
    completedAt?: number;
    attributes?: Record<string, unknown>;
    scheduleName?: string;
    debounceDeadlineEpochMS?: number;
    isDebounced?: boolean;
}
export interface EnqueueOptions {
    deduplicationID?: string;
    priority?: number;
    queuePartitionKey?: string;
    applicationVersion?: string;
    delaySeconds?: number;
    debounceDeadlineEpochMS?: number;
    isDebounced?: boolean;
}
export interface DebounceParams {
    workflowName: string;
    workflowClassName: string;
    queueName: string;
    deduplicationID: string;
    delayUntilEpochMS: number;
    input: string | null;
    serialization: string | null;
}
export interface DebounceResult {
    bouncedWorkflowID: string | null;
    holderWorkflowID: string | null;
    holderIsDebounced: boolean;
    holderWorkflowName: string | null;
    holderWorkflowClassName: string | null;
}
export type DuplicationPolicy = 'reject' | 'return-existing';
export interface ExistenceCheck {
    exists: boolean;
}
export interface MetricData {
    metricType: string;
    metricName: string;
    value: number;
}
export declare function grantDbosSchemaPermissions(databaseUrl: string, roleName: string, logger: GlobalLogger, schemaName?: string): Promise<void>;
export declare function ensureSystemDatabase(sysDbUrl: string, logger: GlobalLogger, customPool?: Pool, schemaName?: string, useListenNotify?: boolean): Promise<void>;
declare class NotificationMap<T> {
    map: Map<string, Map<number, (event?: T) => void>>;
    curCK: number;
    registerCallback(key: string, cb: (event?: T) => void): {
        key: string;
        ck: number;
    };
    deregisterCallback(k: {
        key: string;
        ck: number;
    }): void;
    callCallbacks(key: string, event?: T): void;
}
/**
 * General notes:
 *   The responsibilities of the `SystemDatabase` are to store data for workflows, and
 *     associated steps, transactions, messages, and events.  The system DB is
 *     also the IPC mechanism that performs notifications when things change, for
 *     example a receive is unblocked when a send occurs, or a cancel interrupts
 *     the receive.
 *   The `SystemDatabase` expects values in inputs/outputs/errors to be JSON.  However,
 *     the serialization process of turning data into JSON or converting it back, should
 *     be done elsewhere (executor), as it may require application-specific logic or extensions.
 */
export declare class SystemDatabase {
    #private;
    readonly systemDatabaseUrl: string;
    readonly logger: GlobalLogger;
    readonly serializer: DBOSSerializer;
    readonly pool: Pool;
    readonly schemaName: string;
    notificationsClient: PoolClient | null;
    private notificationsErrorHandler;
    private destroying;
    dbPollingIntervalResultMs: number;
    dbPollingIntervalEventMs: number;
    shouldUseDBNotifications: boolean;
    readonly wakeNotificationsEnabled: boolean;
    readonly notificationsMap: NotificationMap<void>;
    readonly workflowEventsMap: NotificationMap<void>;
    readonly streamsMap: NotificationMap<void>;
    readonly queueWakeMap: NotificationMap<string>;
    readonly completionMap: NotificationMap<void>;
    customPool: boolean;
    readonly notificationCoalesceMs: number;
    private pendingNotifications;
    /**
     * Caps how many DB-backed polling reads (from wait operations) may run
     * concurrently against the pool, so a polling storm cannot check out every
     * client and starve control-plane operations. See {@link #pollWithLimiter}.
     */
    readonly pollLimiter: Semaphore;
    readonly runningWorkflowMap: Map<string, {
        promise: Promise<unknown>;
        queueName?: string;
        queuePartitionKey?: string;
    }>;
    constructor(systemDatabaseUrl: string, logger: GlobalLogger, serializer: DBOSSerializer, sysDbPoolSize?: number, systemDatabasePool?: Pool, schemaName?: string, useListenNotify?: boolean, pollingConcurrency?: number, notificationCoalesceMs?: number, wakeNotificationsEnabled?: boolean);
    getSerializer(): DBOSSerializer;
    init(): Promise<void>;
    destroy(): Promise<void>;
    initWorkflowStatus(initStatus: WorkflowStatusInternal, ownerXid: string | null, options?: {
        isRecoveryRequest?: boolean;
        isDequeuedRequest?: boolean;
        maxRetries?: number;
    }): Promise<{
        status: string;
        shouldExecuteOnThisExecutor: boolean;
        deadlineEpochMS?: number;
        serialization: SysDBSerializationFormat | null;
    }>;
    /**
     * Batch-insert ENQUEUED workflow status rows in a single transaction.
     *
     * Rows whose workflow_uuid already exists are skipped rather than updated, making this
     * idempotent under redelivery (e.g. Kafka). Returns the IDs of the rows actually inserted.
     *
     * Deliberately not `@dbRetry()`-decorated, unlike its neighbours: that loop is unabortable, so a
     * connection outage would trap the caller in it rather than let it back off and observe a
     * shutdown. Callers retry this themselves.
     */
    enqueueWorkflows(statuses: WorkflowStatusInternal[]): Promise<Set<string>>;
    recordWorkflowOutput(workflowID: string, status: WorkflowStatusInternal): Promise<void>;
    recordWorkflowError(workflowID: string, status: WorkflowStatusInternal): Promise<void>;
    getPendingWorkflows(executorID: string, appVersion: string): Promise<GetPendingWorkflowsOutput[]>;
    reenqueuePendingQueuedWorkflows(executorID: string, appVersion: string): Promise<string[]>;
    getWorkflowStatus(workflowID: string, callerID?: string, callerFN?: number): Promise<WorkflowStatusInternal | null>;
    setWorkflowStatus(workflowID: string, status: (typeof StatusString)[keyof typeof StatusString], resetRecoveryAttempts: boolean, internalOptions?: {
        updateName?: string;
    }): Promise<void>;
    getOperationResultAndThrowIfCancelled(workflowID: string, functionID: number): Promise<SystemDatabaseStoredResult | undefined>;
    getAllOperationResults(workflowID: string, limit?: number, offset?: number): Promise<operation_outputs[]>;
    recordOperationResult(workflowID: string, functionID: number, functionName: string, checkConflict: boolean, startTimeEpochMs: number, endTimeEpochMs: number, options?: {
        childWorkflowID?: string | null;
        output?: string | null;
        error?: string | null;
        serialization?: string | null;
    }): Promise<void>;
    runTransactionalStep(workflowID: string, functionID: number, functionName: string, callback: (client: PoolClient) => Promise<string | null>): Promise<SystemDatabaseStoredResult | undefined>;
    checkPatch(workflowID: string, functionID: number, patchName: string, deprecated: boolean): Promise<{
        isPatched: boolean;
        hasEntry: boolean;
    }>;
    cancelWorkflows(workflowIDs: string[], cancelChildren?: boolean): Promise<void>;
    checkIfCanceled(workflowID: string): Promise<void>;
    resumeWorkflows(workflowIDs: string[], queueName?: string): Promise<void>;
    setWorkflowPriority(workflowID: string, priority: number): Promise<void>;
    setWorkflowDelay(workflowID: string, delayUntilEpochMS: number): Promise<void>;
    /**
     * Extend an existing debounced DELAYED workflow's delay and update its inputs, atomically.
     * The new delay is capped at the workflow's debounce_deadline_epoch_ms, if one is set.
     * Matching on workflow name and class ensures a debounce-key collision between different
     * workflows never overwrites another workflow's inputs. If nothing matched, returns the
     * current holder (or that the key is unheld) so the caller can start fresh or surface a conflict.
     * Runs on `client` if given, joining its transaction (e.g. a transactional step's);
     * otherwise in its own retried transaction.
     */
    debounceDelayedWorkflow(params: DebounceParams, client?: PoolClient): Promise<DebounceResult>;
    private debounceDelayedWorkflowStandalone;
    getWorkflowChildren(workflowID: string): Promise<string[]>;
    deleteWorkflows(workflowIDs: string[], deleteChildren?: boolean): Promise<void>;
    forkWorkflow(workflowID: string, startStep: number, options?: {
        newWorkflowID?: string;
        applicationVersion?: string;
        timeoutMS?: number;
        queueName?: string;
        queuePartitionKey?: string;
        replacementChildren?: Record<string, string>;
    }): Promise<string>;
    forkFromFailure(workflowIDs: string[], options?: {
        applicationVersion?: string;
        queueName?: string;
        queuePartitionKey?: string;
        fromLastFailure?: boolean;
        fromLastStep?: boolean;
        fromStep?: number;
        fromStepName?: string;
    }): Promise<string[]>;
    private bulkForkWorkflows;
    exportWorkflow(workflowID: string, exportChildren?: boolean): Promise<ExportedWorkflow[]>;
    importWorkflow(workflows: ExportedWorkflow[]): Promise<void>;
    registerRunningWorkflow(workflowID: string, workflowPromise: Promise<unknown>, onSettled: () => void, queueName?: string, queuePartitionKey?: string): void;
    checkForRunningWorkflow(workflowID: string): boolean;
    clearRunningWorkflow(workflowID: string): void;
    countRunningWorkflowsForQueue(queueName: string, queuePartitionKey?: string): number;
    awaitRunningWorkflows(): Promise<void>;
    awaitWorkflowResult(workflowID: string, timeoutSeconds?: number, callerID?: string, timerFuncID?: number, pollingIntervalMs?: number): Promise<SystemDatabaseStoredResult | undefined>;
    awaitFirstWorkflowId(workflowIds: string[], callerID?: string, pollingIntervalMs?: number): Promise<string>;
    awaitWorkflowIds(workflowIds: string[], callerID?: string, pollingIntervalMs?: number): Promise<void>;
    durableSleepms(workflowID: string, functionID: number, durationMS: number): Promise<void>;
    readonly nullTopic = "__null__topic__";
    send(workflowID: string, functionID: number, destinationID: string, message: string | null, topic: string | undefined, serialization: string | null, idempotencyKey?: string): Promise<void>;
    sendDirect(destinationID: string, message: string | null, topic: string | undefined, serialization: string | null, idempotencyKey?: string): Promise<void>;
    recv(workflowID: string, functionID: number, timeoutFunctionID: number, topic?: string, timeoutSeconds?: number, pollingIntervalMs?: number): Promise<{
        serializedValue: string | null;
        serialization: string | null;
    }>;
    setEvent(workflowID: string, functionID: number, key: string, message: string | null, serialization: string | null): Promise<void>;
    getEvent(workflowID: string, key: string, timeoutSeconds: number, callerWorkflow?: {
        workflowID: string;
        functionID: number;
        timeoutFunctionID: number;
    }, pollingIntervalMs?: number): Promise<{
        serializedValue: string | null;
        serialization: string | null;
    }>;
    getEventDispatchState(service: string, workflowName: string, key: string): Promise<DBOSExternalState | undefined>;
    upsertEventDispatchState(state: DBOSExternalState): Promise<DBOSExternalState>;
    writeStreamFromStep(workflowID: string, functionID: number, key: string, serializedValue: string, serialization: string | null): Promise<void>;
    writeStreamFromWorkflow(workflowID: string, functionID: number, key: string, serializedValue: string, serialization: string | null, functionName: string): Promise<void>;
    closeStream(workflowID: string, functionID: number, key: string): Promise<void>;
    readStreamValue(workflowID: string, key: string, offset: number): Promise<{
        status: string | null;
        value: {
            serializedValue: string;
            serialization: string | null;
        } | undefined;
    }>;
    /**
     * Register the queue scheduler's wake callback so an ENQUEUED-transition NOTIFY can trip its
     * dispatch loop before the next poll. The callback receives the woken queue's name. Returns a
     * deregistration handle, or `undefined` when wakes are disabled (the caller then relies on the
     * poll floor). Symmetric to how recv/getEvent register on their notification maps.
     */
    registerQueueWake(cb: (queueName?: string) => void): {
        key: string;
        ck: number;
    } | undefined;
    deregisterQueueWake(handle: {
        key: string;
        ck: number;
    } | undefined): void;
    flushNotifications(): Promise<void>;
    getAllEvents(workflowID: string): Promise<Record<string, unknown>>;
    getAllNotifications(workflowID: string): Promise<{
        topic: string | null;
        message: unknown;
        createdAtEpochMs: number;
        consumed: boolean;
    }[]>;
    getAllStreamEntries(workflowID: string): Promise<Record<string, unknown[]>>;
    transitionDelayedWorkflows(): Promise<void>;
    clearQueueAssignment(workflowID: string): Promise<boolean>;
    getDeduplicatedWorkflow(queueName: string, deduplicationID: string): Promise<string | null>;
    getQueuePartitions(queueName: string): Promise<string[]>;
    findAndMarkStartableWorkflows(queue: WorkflowQueue, executorID: string, appVersion: string, queuePartitionKey?: string, descendantsOnly?: boolean): Promise<string[]>;
    listWorkflows(input: GetWorkflowsInput): Promise<WorkflowStatusInternal[]>;
    getWorkflowAggregates(input: GetWorkflowAggregatesInput): Promise<WorkflowAggregateRow[]>;
    getStepAggregates(input: GetStepAggregatesInput): Promise<StepAggregateRow[]>;
    garbageCollect(cutoffEpochTimestampMs?: number, rowsThreshold?: number): Promise<void>;
    getMetrics(startTime: string, endTime: string): Promise<MetricData[]>;
    createSchedule(schedule: WorkflowScheduleInternal, client?: PoolClient): Promise<void>;
    listSchedules(filters?: {
        status?: string | string[];
        workflowName?: string | string[];
        scheduleNamePrefix?: string | string[];
    }, client?: PoolClient): Promise<WorkflowScheduleInternal[]>;
    getSchedule(name: string, client?: PoolClient): Promise<WorkflowScheduleInternal | null>;
    deleteSchedule(name: string, client?: PoolClient): Promise<void>;
    setScheduleStatus(name: string, status: string, client?: PoolClient): Promise<void>;
    updateSchedule(name: string, updates: WorkflowScheduleUpdate, client?: PoolClient): Promise<void>;
    updateLastFiredAt(name: string, lastFiredAt: string): Promise<void>;
    applySchedules(schedules: WorkflowScheduleInternal[]): Promise<void>;
    createApplicationVersion(versionName: string): Promise<void>;
    updateApplicationVersionTimestamp(versionName: string, newTimestamp: number): Promise<void>;
    listApplicationVersions(): Promise<VersionInfo[]>;
    getLatestApplicationVersion(): Promise<VersionInfo>;
    getQueue(name: string): Promise<QueueRecord | null>;
    listQueues(): Promise<QueueRecord[]>;
    deleteQueue(name: string): Promise<void>;
    updateQueue(name: string, fields: QueueRecordUpdate): Promise<void>;
    /** Returns true iff this call inserted a new row (i.e. the queue did not
     * previously exist). False if the row already existed, regardless of
     * whether it was updated. */
    upsertQueue(record: QueueRecord, updateExisting: boolean): Promise<boolean>;
    private insertWorkflowStatus;
    private getWorkflowStatusValue;
    private updateWorkflowStatus;
    private recordOperationResultInternal;
    /**
     * A background process that listens for notifications from Postgres then signals the appropriate
     * workflow listener by resolving its promise.
     */
    reconnectTimeout: NodeJS.Timeout | null;
}
export {};
//# sourceMappingURL=system_database.d.ts.map